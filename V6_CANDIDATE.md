# v6 candidate — bleed control, NOT a claimed edge

Branch only. `main` is untouched; revert is `git checkout main` (or delete the branch).

## What changed (3 defaults, all env-overridable)
| Setting | Was | Now | Measured basis |
|---|---|---|---|
| FOMO_SIZE_MARGINAL | 75 | 25 | $75-100 tier: 3 wins / 30 trades, -$386 |
| FOMO_SIZE_CROSSCLAN | 150 | 50 | $150 tier: 2 wins / 32 trades, -$450 |
| FOMO_TIME_STOP_H | 72 | 24 | majority_exit avg -9.4% over 57 trades; capital sat dead |
| FOMO_TIME_STOP_MIN | 20 | 10 | same |
| FOMO_REENTRY_COOLDOWN_D | none (same-cycle only) | 7 | WORM: 32 entries, -$534 = 66% of all losses |

## What it does NOT do
It does not add an edge. The backtest says no entry rule tested survives realistic fills
(-33.51%/trade at 15-min lag + 4% costs). This branch only stops the specific leaks that
were measured: repeated re-entry into the same failing token, and oversizing the worst signals.

## Evidence the exit is not the bottleneck
probeExitability (2026-11-24, 5,412 usable events, 76 clan-3rd-buyer signals):
94.6% had a trade inside 30 minutes, 91.9% could exit at better than -20%, median move +3.31%,
median counterparty print $10,230. Liquidity exists; the 4% round trip eats the median move.

## Merge criteria
Run 2 weeks on the branch state or backtest it; merge only if expectancy beats the current
-$10.27/trade baseline with fees modelled.
