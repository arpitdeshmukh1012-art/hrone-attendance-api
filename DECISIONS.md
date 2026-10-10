# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (duplicate creates, every lookup), `{department, emp_code}` (filtered,
   sorted list; trend), `{joined_on}` (summary headcount). `attendance_logs`: unique `{emp_code, date}` (one record per
   day, per-employee month scans, the `$lookup`s, punch-out) and `{date:-1, emp_code:1}` (list sort, month scans for the
   leaderboard). I rejected `{emp_code, punch_in}` for punch-out: bounding `date <= today` lets the unique index do it.

2. **Punch-in race.** Both requests read the employee and compute the same `(emp_code, date)`. Both call `insert_one`
   and there is no read first. The unique index lets exactly one insert succeed. The other gets `DuplicateKeyError`,
   which I turn into 409.

3. **Ties.** `$rank` gives competition ranks (1, 2, 2, 4), and I filter on `rank <= limit`. With `limit=2` and two
   employees tied for 2nd, both are returned (3 rows), sorted by `emp_code`. Cutting at `limit` rows would drop one of
   them arbitrarily.

4. **Headcount.** The pipeline starts from `employees` (`joined_on <= month end`) and left-joins that month's logs with
   `$lookup`. An employee with no logs still produces a row with an empty array, so the employee is counted.

5. **At 100x.** I would pre-aggregate daily per-department and monthly per-employee totals, updated on every write.
   Analytics would then read a few hundred rows instead of scanning raw logs.
