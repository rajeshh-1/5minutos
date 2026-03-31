# Stability Summary

- runtime_requirement_minutes: 60
- rows_total: 60
- rows_go: 0

## Top 10 by Robustness

1. `tp1.75_t800_uw35.0_cut15` | score=-0.15 | go=NO_GO | pnl/trade=0.0 | hedge_failed=1.0 | dd=0.0%
2. `tp2.00_t800_uw25.0_cut15` | score=-0.15 | go=NO_GO | pnl/trade=0.0 | hedge_failed=1.0 | dd=0.0%
3. `tp2.25_t800_uw15.0_cut15` | score=-0.15 | go=NO_GO | pnl/trade=0.0 | hedge_failed=1.0 | dd=0.0%
4. `tp2.00_t1000_uw25.0_cut30` | score=-0.1627259259 | go=NO_GO | pnl/trade=-0.0157407407 | hedge_failed=1.0 | dd=4.72222222%
5. `tp2.00_t1200_uw40.0_cut45` | score=-0.1627259259 | go=NO_GO | pnl/trade=-0.0157407407 | hedge_failed=1.0 | dd=4.72222222%
6. `tp2.00_t1400_uw50.0_cut60` | score=-0.1627259259 | go=NO_GO | pnl/trade=-0.0157407407 | hedge_failed=1.0 | dd=4.72222222%
7. `tp2.00_t1600_uw60.0_cut75` | score=-0.1627259259 | go=NO_GO | pnl/trade=-0.0157407407 | hedge_failed=1.0 | dd=4.72222222%
8. `tp2.50_t1000_uw25.0_cut30` | score=-0.1627259259 | go=NO_GO | pnl/trade=-0.0157407407 | hedge_failed=1.0 | dd=4.72222222%
9. `tp2.50_t1200_uw40.0_cut45` | score=-0.1627259259 | go=NO_GO | pnl/trade=-0.0157407407 | hedge_failed=1.0 | dd=4.72222222%
10. `tp2.50_t1400_uw50.0_cut60` | score=-0.1627259259 | go=NO_GO | pnl/trade=-0.0157407407 | hedge_failed=1.0 | dd=4.72222222%

## Recommended Profiles

- conservative: `tp1.75_t800_uw35.0_cut15` (score=-0.15, go=NO_GO, pnl/trade=0.0, dd=0.0%)
- moderate: `tp1.75_t800_uw35.0_cut15` (score=-0.15, go=NO_GO, pnl/trade=0.0, dd=0.0%)
- aggressive: `tp1.75_t800_uw35.0_cut15` (score=-0.15, go=NO_GO, pnl/trade=0.0, dd=0.0%)

## Notes

- Stability penalty is already embedded in `robustness_score` via pnl/std and hedge-success/std terms.
- If `rows_go` is 0, recommendations are best-effort over NO_GO candidates.
