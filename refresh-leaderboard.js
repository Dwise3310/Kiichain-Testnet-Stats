// /api/refresh-leaderboard.js
//
// Vercel serverless function. Triggered by Vercel Cron (see vercel.json) and
// can also be hit manually (with the correct secret) to force a refresh.
//
// What it does, every run:
//   1. Reads how far we scanned last time (oro_leaderboard_meta.last_scanned_block).
//      On the very first-ever run, that's 0, so it finds the block closest to
//      the testnet start date instead of scanning from genesis.
//   2. Scans forward from there, in chunks, collecting ORO Transfer events.
//   3. For every unique address touched by those transfers, re-checks its
//      CURRENT balance (a transfer only tells you something moved, not the
//      final balance — so we always re-read balanceOf for touched addresses).
//   4. Upserts those balances into oro_leaderboard.
//   5. Saves how far we got, so next run continues from there.
//
// Each invocation only processes a bounded number of blocks / time budget,
// so it stays within serverless timeout limits even on a big first run.
// Run it enough times (cron handles this) and it naturally catches up to
// the chain tip, then just does small incremental updates forever after.

import { ethers } from 'ethers';
import { createClient } from '@supabase/supabase-js';

const RPC_URL = 'https://json-rpc.uno.sentry.testnet.v3.kiivalidator.com/';
const ORO_TOKEN_ADDRESS = '0x5a47EF9C19dae206e99382955eb9eD5ca510A7Fa';
const TESTNET_START_DATE = '2025-06-24T00:00:00Z'; // told to us — approximate, that's fine

const TRANSFER_TOPIC = ethers.id('Transfer(address,address,uint256)');
const LOG_CHUNK_SIZE = 8000;       // blocks per single eth_getLogs call
const CONCURRENCY = 8;             // parallel eth_getLogs calls per batch
const MAX_BLOCKS_PER_RUN = 5000000; // safety cap so one invocation can't run forever
const TIME_BUDGET_MS = 55000;      // bail out early if we're close to the function timeout
const BALANCE_BATCH_SIZE = 15;     // concurrent balanceOf calls

const ERC20_ABI = [
  'function balanceOf(address owner) view returns (uint256)',
  'function decimals() view returns (uint8)',
];

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

function jsonAddress(topic) {
  return ethers.getAddress('0x' + topic.slice(26));
}

// Binary-search block numbers by timestamp to find the block closest to a target date.
async function findBlockByTimestamp(provider, targetUnixSeconds) {
  const latest = await provider.getBlockNumber();
  let lo = 0;
  let hi = latest;
  let best = 0;

  const latestBlock = await provider.getBlock(latest);
  if (latestBlock.timestamp <= targetUnixSeconds) return latest;

  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    const block = await provider.getBlock(mid);
    if (!block) break;
    if (block.timestamp < targetUnixSeconds) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return best;
}

async function getStartBlock(provider) {
  const { data, error } = await supabase
    .from('oro_leaderboard_meta')
    .select('last_scanned_block')
    .eq('id', 1)
    .single();

  if (error) throw new Error('Failed to read meta: ' + error.message);

  if (data.last_scanned_block && data.last_scanned_block > 0) {
    return data.last_scanned_block + 1;
  }

  // First-ever run: find the block nearest the testnet start date.
  const targetUnix = Math.floor(new Date(TESTNET_START_DATE).getTime() / 1000);
  const block = await findBlockByTimestamp(provider, targetUnix);
  return block;
}

async function fetchWindow(provider, fromBlock, toBlock) {
  try {
    return await provider.getLogs({
      address: ORO_TOKEN_ADDRESS,
      topics: [TRANSFER_TOPIC],
      fromBlock,
      toBlock,
    });
  } catch (err) {
    // RPC rejected this range (too large, or a transient error) — split and retry once.
    if (toBlock - fromBlock > 200) {
      const mid = Math.floor((fromBlock + toBlock) / 2);
      const [a, b] = await Promise.all([
        fetchWindow(provider, fromBlock, mid),
        fetchWindow(provider, mid + 1, toBlock),
      ]);
      return [...a, ...b];
    }
    console.error(`Failed to fetch logs for blocks ${fromBlock}-${toBlock}:`, err.message);
    return [];
  }
}

// Scans [fromBlock, toBlock] in parallel batches of CONCURRENCY windows at a time.
// Returns { logs, scannedTo } — scannedTo is the last block fully covered by a
// completed batch, so partial progress is always safe to save even if we run
// out of time budget mid-scan.
async function scanRange(provider, fromBlock, toBlock, timeBudgetMs, startedAt) {
  const windows = [];
  for (let start = fromBlock; start <= toBlock; start += LOG_CHUNK_SIZE) {
    windows.push([start, Math.min(start + LOG_CHUNK_SIZE - 1, toBlock)]);
  }

  const allLogs = [];
  let scannedTo = fromBlock - 1;

  for (let i = 0; i < windows.length; i += CONCURRENCY) {
    if (Date.now() - startedAt > timeBudgetMs) break;

    const batch = windows.slice(i, i + CONCURRENCY);
    const results = await Promise.all(
      batch.map(([s, e]) => fetchWindow(provider, s, e))
    );
    results.forEach((logs) => allLogs.push(...logs));
    scannedTo = batch[batch.length - 1][1]; // end of last window in this batch
  }

  return { logs: allLogs, scannedTo };
}

async function batchGetBalances(contract, addresses) {
  const results = new Map();
  for (let i = 0; i < addresses.length; i += BALANCE_BATCH_SIZE) {
    const batch = addresses.slice(i, i + BALANCE_BATCH_SIZE);
    const balances = await Promise.all(
      batch.map((addr) =>
        contract.balanceOf(addr).catch(() => null)
      )
    );
    batch.forEach((addr, idx) => {
      if (balances[idx] !== null) results.set(addr, balances[idx]);
    });
  }
  return results;
}

export default async function handler(req, res) {
  // Simple shared-secret gate so randoms on the internet can't trigger scans
  // (each run costs RPC calls). Vercel Cron sends this header automatically
  // when CRON_SECRET is set as an env var; manual calls need ?secret=... to match.
  const authHeader = req.headers['authorization'];
  const providedSecret = req.query?.secret;
  const expectedSecret = process.env.CRON_SECRET;

  if (expectedSecret) {
    const authorized =
      authHeader === `Bearer ${expectedSecret}` || providedSecret === expectedSecret;
    if (!authorized) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  const startedAt = Date.now();
  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const contract = new ethers.Contract(ORO_TOKEN_ADDRESS, ERC20_ABI, provider);

  try {
    const latestBlock = await provider.getBlockNumber();
    const fromBlock = await getStartBlock(provider);

    if (fromBlock > latestBlock) {
      return res.status(200).json({ status: 'already up to date', latestBlock });
    }

    const toBlock = Math.min(fromBlock + MAX_BLOCKS_PER_RUN, latestBlock);

    const touchedAddresses = new Set();

    const { logs, scannedTo } = await scanRange(
      provider,
      fromBlock,
      toBlock,
      TIME_BUDGET_MS,
      startedAt
    );

    for (const log of logs) {
      touchedAddresses.add(jsonAddress(log.topics[1])); // from
      touchedAddresses.add(jsonAddress(log.topics[2])); // to
    }

    // Zero address isn't a real holder (mint/burn source) — drop it.
    touchedAddresses.delete(ethers.ZeroAddress);

    let decimals = 18;
    try {
      decimals = await contract.decimals();
    } catch {
      // fall back to 18 if the call fails
    }

    if (touchedAddresses.size > 0) {
      const balances = await batchGetBalances(contract, [...touchedAddresses]);

      const rows = [...balances.entries()].map(([address, rawBalance]) => ({
        address,
        balance: parseFloat(ethers.formatUnits(rawBalance, decimals)),
        updated_at: new Date().toISOString(),
      }));

      if (rows.length > 0) {
        const { error: upsertError } = await supabase
          .from('oro_leaderboard')
          .upsert(rows, { onConflict: 'address' });
        if (upsertError) throw new Error('Upsert failed: ' + upsertError.message);
      }
    }

    const { error: metaError } = await supabase
      .from('oro_leaderboard_meta')
      .update({
        last_scanned_block: scannedTo,
        last_run_at: new Date().toISOString(),
        last_run_status: 'ok',
      })
      .eq('id', 1);
    if (metaError) throw new Error('Meta update failed: ' + metaError.message);

    return res.status(200).json({
      status: 'ok',
      scannedFrom: fromBlock,
      scannedTo,
      latestBlock,
      addressesTouched: touchedAddresses.size,
      caughtUp: scannedTo >= latestBlock,
    });
  } catch (err) {
    console.error(err);
    await supabase
      .from('oro_leaderboard_meta')
      .update({ last_run_at: new Date().toISOString(), last_run_status: 'error: ' + err.message })
      .eq('id', 1);
    return res.status(500).json({ error: err.message });
  }
}
