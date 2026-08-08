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
  const address = req.query?.address;

  if (!address || !ethers.isAddress(address)) {
    return res.status(400).json({ error: 'Valid wallet address required' });
  }

  try {
    const provider = new ethers.JsonRpcProvider(RPC_URL);
    const contract = new ethers.Contract(ORO_TOKEN_ADDRESS, ERC20_ABI, provider);

    const [rawBalance, decimals] = await Promise.all([
      contract.balanceOf(address),
      contract.decimals().catch(() => 18),
    ]);

    const balance = parseFloat(ethers.formatUnits(rawBalance, decimals));
    const checksummedAddress = ethers.getAddress(address);

    // Only bother storing wallets that actually hold something —
    // no point cluttering the leaderboard with zero-balance rows.
    if (balance > 0) {
      const { error } = await supabase
        .from('oro_leaderboard')
        .upsert(
          { address: checksummedAddress, balance, updated_at: new Date().toISOString() },
          { onConflict: 'address' }
        );
      if (error) throw new Error('Upsert failed: ' + error.message);
    }

    return res.status(200).json({ status: 'ok', address: checksummedAddress, balance });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
