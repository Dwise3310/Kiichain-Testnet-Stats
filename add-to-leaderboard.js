// /api/add-to-leaderboard.js
//
// Called automatically whenever someone searches a wallet on the main site.
// Deliberately does NOT trust any balance number sent from the browser —
// it re-fetches the real balance itself from the chain before saving,
// so nobody can fake their way onto the leaderboard via dev tools.

import { ethers } from 'ethers';
import { createClient } from '@supabase/supabase-js';

const RPC_URL = 'https://json-rpc.uno.sentry.testnet.v3.kiivalidator.com/';
const ORO_TOKEN_ADDRESS = '0x5a47EF9C19dae206e99382955eb9eD5ca510A7Fa';

const ERC20_ABI = [
  'function balanceOf(address owner) view returns (uint256)',
  'function decimals() view returns (uint8)',
];

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

export default async function handler(req, res) {
  // GET ?address=0x... — single address (used by the main search page)
  if (req.method === 'GET') {
    const address = req.query?.address;
    if (!address || !ethers.isAddress(address)) {
      return res.status(400).json({ error: 'Valid wallet address required' });
    }
    try {
      const result = await checkAndStore(address);
      return res.status(200).json({ status: 'ok', ...result });
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: err.message });
    }
  }

  // POST { addresses: ["0x...", "0x...", ...] } — bulk mode, up to 200 per call
  if (req.method === 'POST') {
    const addresses = req.body?.addresses;
    if (!Array.isArray(addresses) || addresses.length === 0) {
      return res.status(400).json({ error: 'addresses array required in request body' });
    }
    if (addresses.length > 200) {
      return res.status(400).json({ error: 'Max 200 addresses per request — split into smaller batches' });
    }

    const validAddresses = [...new Set(addresses)].filter((a) => ethers.isAddress(a));
    const invalidCount = addresses.length - validAddresses.length;

    const BATCH_CONCURRENCY = 20;
    const results = { added: 0, zeroBalance: 0, errors: 0 };

    for (let i = 0; i < validAddresses.length; i += BATCH_CONCURRENCY) {
      const chunk = validAddresses.slice(i, i + BATCH_CONCURRENCY);
      const chunkResults = await Promise.all(
        chunk.map((addr) =>
          checkAndStore(addr).catch((err) => {
            console.error(`Failed for ${addr}:`, err.message);
            return { error: true };
          })
        )
      );
      chunkResults.forEach((r) => {
        if (r.error) results.errors++;
        else if (r.balance > 0) results.added++;
        else results.zeroBalance++;
      });
    }

    return res.status(200).json({
      status: 'ok',
      processed: validAddresses.length,
      invalidSkipped: invalidCount,
      ...results,
    });
  }

  return res.status(405).json({ error: 'Method not allowed' });
}

async function checkAndStore(address) {
  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const contract = new ethers.Contract(ORO_TOKEN_ADDRESS, ERC20_ABI, provider);

  const [rawBalance, decimals] = await Promise.all([
    contract.balanceOf(address),
    contract.decimals().catch(() => 18),
  ]);

  const balance = parseFloat(ethers.formatUnits(rawBalance, decimals));
  const checksummedAddress = ethers.getAddress(address);

  if (balance > 0) {
    const { error } = await supabase
      .from('oro_leaderboard')
      .upsert(
        { address: checksummedAddress, balance, updated_at: new Date().toISOString() },
        { onConflict: 'address' }
      );
    if (error) throw new Error('Upsert failed: ' + error.message);
  }

  return { address: checksummedAddress, balance };
}
