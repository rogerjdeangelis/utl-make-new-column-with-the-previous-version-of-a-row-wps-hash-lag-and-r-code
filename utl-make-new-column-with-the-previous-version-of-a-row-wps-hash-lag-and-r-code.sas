%let pgm=utl-make-new-column-with-the-previous-version-of-a-row-wps-hash-lag-and-r-code;

A problem for wps R not wps base or sql or hash. Make a new column with the previous version of a row

'Keeping a "lagged" value of ID for each of the unique values of TAGS.'
Keintz, Mark
mkeintz@outlook.com

SOLUTIONS

   1 wps hash
     Keintz, Mark
     mkeintz@outlook.com
     note: h.replace(key:tags,data:id);

   2 wps lag w/o do_over
     Keintz, Mark
     mkeintz@outlook.com (nice observation)

     I put "lagged" in quotes because this example, more than many, suggests the LAG function
     is misleadingly named.  I find it more useful to think of it as UFQ (Update Fifo Queue),
     and each of the WHEN clauses above as referring to a unique queue.  To my knowledge,
     LAG and DIF are the only functions for which all those WHEN
     conditions could not be collapsed into a single clause, normally
     a good programming practice.  You would NOT get the same results by using

     when ('A1','A2','B2','C3','C4','D4','A4') newcol=lag(id);

   4 wps r

HASH repos on end


Note: About do_over macro.

    1. In some cases it may not scale. In some cases it may be faster then loops.
       A lot depends on the compiler? Compilers love repeated code.

    2. You can use do_over to generate the code and insert the code in your program.
       Probably better than manually typing or repeated edits.

github
https://tinyurl.com/5n7uscyk
https://github.com/rogerjdeangelis/utl-make-new-column-with-the-previous-version-of-a-row-wps-hash-lag-and-r-code

stackoverflow R
https://tinyurl.com/2hksv6vc
https://stackoverflow.com/questions/77293629/how-to-make-a-new-column-with-the-previous-version-of-a-row

Solution by
https://stackoverflow.com/users/3358272/r2evans

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input ID TAGS $2.;
cards;
1 A1
1 A2
2 A1
2 B2
2 C3
2 C4
3 A1
3 D4
3 C3
3 A4
3 B2
4 A2
4 B2
;;;;
run;quit;

/**************************************************************************************************************************/
/*                         |                                    |                                                         */
/* INPUT                   |              PROCESS               |      OUTPUT                                             */
/*                         |                                    |                                                         */
/* SD1.HAVE total obs=13   |                                    |                                                         */
/*                         |                                    |                                                         */
/*  ID TAGS                |  PREID                             |     ID   TAGS   PREID                                   */
/*                         |                                    |                                                         */
/*   1   A1                |     NA  PREID=NA no earlier  A1    |      1     A1      NA                                   */
/*   1   A2                |     NA  PREID=NA no earlier  A2    |      1     A2      NA                                   */
/*   2   A1                |      1  PREID=1 for previous A1    |      2     A1       1                                   */
/*   2   B2                |     NA  PREID=NA no earlier  B2    |      2     B2      NA                                   */
/*   2   C3                |     NA  PREID=NA no earlier  C3    |      2     C3      NA                                   */
/*   2   C4                |     NA  PREID=NA no earlier  C4    |      2     C4      NA                                   */
/*   3   A1                |      2  PREID=2 for previous A1    |      3     A1       2                                   */
/*   3   D4                |     NA  PREID=NA no earlier  D4    |      3     D4      NA                                   */
/*   3   C3                |      2  PREID=2 for previous C3    |      3     C3       2                                   */
/*   3   A4                |     NA  PREID=NA no earlier  A4    |      3     A4      NA                                   */
/*   3   B2                |      2  PREID=2 for previous B2    |      3     B2       2                                   */
/*   4   A2                |      1  PREID=1 for previous A2    |      4     A2       1                                   */
/*   4   B2                |      3  PREID=3 for previous B2    |      4     B2       3                                   */
/*                         |                                    |                                                         */
/**************************************************************************************************************************/

/*                        _               _
/ | __      ___ __  ___  | |__   __ _ ___| |__
| | \ \ /\ / / `_ \/ __| | `_ \ / _` / __| `_ \
| |  \ V  V /| |_) \__ \ | | | | (_| \__ \ | | |
|_|   \_/\_/ | .__/|___/ |_| |_|\__,_|___/_| |_|
             |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want_hash; run;quit;

%utl_submit_wps64x('

libname sd1 "d:/sd1";

data sd1.want_hash;

  set sd1.have;

  if _n_=1 then do;

    declare hash h ();
      h.definekey("tags");
      h.definedata("newcol");
      h.definedone();

  end;

  if h.find()^=0 then newcol=.;

  h.replace(key:tags,data:id);

run;quit;

proc print data=sd1.want_hash;
run;quit;

');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    ID    TAGS    NEWCOL                                                                                            */
/*                                                                                                                        */
/*   1     1     A1        .                                                                                              */
/*   2     1     A2        .                                                                                              */
/*   3     2     A1        1                                                                                              */
/*   4     2     B2        .                                                                                              */
/*   5     2     C3        .                                                                                              */
/*   6     2     C4        .                                                                                              */
/*   7     3     A1        2                                                                                              */
/*   8     3     D4        .                                                                                              */
/*   9     3     C3        2                                                                                              */
/*  10     3     A4        .                                                                                              */
/*  11     3     B2        2                                                                                              */
/*  12     4     A2        1                                                                                              */
/*  13     4     B2        3                                                                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                   __                   _
|___ \  __      ___ __  ___  __      __/ /_      _____    __| | ___     _____   _____ _ __
  __) | \ \ /\ / / `_ \/ __| \ \ /\ / / /\ \ /\ / / _ \  / _` |/ _ \   / _ \ \ / / _ \ `__|
 / __/   \ V  V /| |_) \__ \  \ V  V / /  \ V  V / (_) || (_| | (_) | | (_) \ V /  __/ |
|_____|   \_/\_/ | .__/|___/   \_/\_/_/    \_/\_/ \___/  \__,_|\___/___\___/ \_/ \___|_|
                 |_|                                              |_____|
          _ _   _                 _
__      _(_) |_| |__   ___  _   _| |_
\ \ /\ / / | __| `_ \ / _ \| | | | __|
 \ V  V /| | |_| | | | (_) | |_| | |_
  \_/\_/ |_|\__|_| |_|\___/ \__,_|\__|

*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('

libname sd1 "d:/sd1";

data sd1.want;

  set sd1.have;

  select (tags);
    when ("A1") newcol=lag(id);
    when ("A2") newcol=lag(id);
    when ("A4") newcol=lag(id);
    when ("B2") newcol=lag(id);
    when ("C3") newcol=lag(id);
    when ("C4") newcol=lag(id);
    when ("D4") newcol=lag(id);
  end;

run;quit;

proc print;
run;quit;

');

/*        _ _   _           _
__      _(_) |_| |__     __| | ___     _____   _____ _ __
\ \ /\ / / | __| `_ \   / _` |/ _ \   / _ \ \ / / _ \ `__|
 \ V  V /| | |_| | | | | (_| | (_) | | (_) \ V /  __/ |
  \_/\_/ |_|\__|_| |_|  \__,_|\___/___\___/ \_/ \___|_|
                                 |_____|
*/

/*----                                                                   ----*/
/*----  Create macro array                                               ----*/
/*----                                                                   ----*/

Proc sql;
  select
    distinct tags
  into
    :_tg1-
  from
    sd1.have
;quit;

%put &=sqlobs;

/*----                                                                   ----*/
/*----  Generate code if do not want to use the do_over macro            ----*/
/*----                                                                   ----*/

data _null_;
put %do_over(_tg, phrase= %str("when ('?') newcol=lag(id)" /));
run;quit;

/*---- when ('A1') newcol=lag(id)                                        ----*/
/*---- when ('A2') newcol=lag(id)                                        ----*/
/*---- when ('A4') newcol=lag(id)                                        ----*/
/*---- when ('B2') newcol=lag(id)                                        ----*/
/*---- when ('C3') newcol=lag(id)                                        ----*/
/*---- when ('C4') newcol=lag(id)                                        ----*/
/*---- when ('D4') newcol=lag(id)                                        ----*/

Proc sql;
  select
    distinct tags
  into
    :_tg1-
  from
    sd1.have
;quit;
%put &=sqlobs;


%let _tgn = &sqlobs;

%put &=_tg3;/* _TG3=A4                                                   ----*/
%put &=_tgn;/* _TGN=7                                                    ----*/


proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
data sd1.want;
  set sd1.have;
  select (tags);
    %do_over(_tg, phrase= %str(when ('?') newcol=lag(id);))
    otherwise put tags=;
  end;
run;quit;
proc print;
run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    ID    TAGS    NEWCOL                                                                                           */
/*                                                                                                                        */
/*    1     1     A1        .                                                                                             */
/*    2     1     A2        .                                                                                             */
/*    3     2     A1        1                                                                                             */
/*    4     2     B2        .                                                                                             */
/*    5     2     C3        .                                                                                             */
/*    6     2     C4        .                                                                                             */
/*    7     3     A1        2                                                                                             */
/*    8     3     D4        .                                                                                             */
/*    9     3     C3        2                                                                                             */
/*   10     3     A4        .                                                                                             */
/*   11     3     B2        2                                                                                             */
/*   12     4     A2        1                                                                                             */
/*   13     4     B2        3                                                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____
|___ /  __      ___ __  ___   _ __
  |_ \  \ \ /\ / / `_ \/ __| | `__|
 ___) |  \ V  V /| |_) \__ \ | |
|____/    \_/\_/ | .__/|___/ |_|
                 |_|
*/

/*---- R does not do an internal sort                                    ----*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(dplyr);
want<-have %>%
  mutate(PREID = lag(ID, default = NA), .by = TAGS);
want;
endsubmit;
import data=sd1.want r=want;
run;quit;
');

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                     |                                                                                                  */
/*  The WPS R          |    WPS                                                                                           */
/*                     |                                                                                                  */
/*     ID TAGS PREID   |    obs    ID    TAGS    PREID                                                                    */
/*                     |                                                                                                  */
/*  1   1   A1    NA   |      1     1     A1       .                                                                      */
/*  2   1   A2    NA   |      2     1     A2       .                                                                      */
/*  3   2   A1     1   |      3     2     A1       1                                                                      */
/*  4   2   B2    NA   |      4     2     B2       .                                                                      */
/*  5   2   C3    NA   |      5     2     C3       .                                                                      */
/*  6   2   C4    NA   |      6     2     C4       .                                                                      */
/*  7   3   A1     2   |      7     3     A1       2                                                                      */
/*  8   3   D4    NA   |      8     3     D4       .                                                                      */
/*  9   3   C3     2   |      9     3     C3       2                                                                      */
/*  10  3   A4    NA   |     10     3     A4       .                                                                      */
/*  11  3   B2     2   |     11     3     B2       2                                                                      */
/*  12  4   A2     1   |     12     4     A2       1                                                                      */
/*  13  4   B2     3   |     13     4     B2       3                                                                      */
/*                     |                                                                                                  */
/**************************************************************************************************************************/

/*
 _ __ ___ _ __   ___  ___
| `__/ _ \ `_ \ / _ \/ __|
| | |  __/ |_) | (_) \__ \
|_|  \___| .__/ \___/|___/
         |_|
*/

REPO
------------------------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/distinct-counts-for_3200-variables-and_660-thousand-records-using-HASH-SQL-and-proc-freq
https://github.com/rogerjdeangelis/utl-append-and-split-tables-into-two-tables-one-with-common-variables-and-one-without-dosubl-hash
https://github.com/rogerjdeangelis/utl-are-the-files-identical-or-was-the-file-corrupted-durring-transfer-hash
https://github.com/rogerjdeangelis/utl-average-nap-time-for-three-babies-in-and-unsorted-table-using-a-hash-and-r
https://github.com/rogerjdeangelis/utl-count-distinct-compound-keys-using-sql-and-hash-algorithms
https://github.com/rogerjdeangelis/utl-create-a-list-of-male-students-at-achme-high-school-using_a_hash
https://github.com/rogerjdeangelis/utl-create-a-state-diagram-table-hash-corresp-and-transpose
https://github.com/rogerjdeangelis/utl-creating-two-tables-sum-of-weight-by-age-and-by-sex-using-a-hash-of-hashes_hoh
https://github.com/rogerjdeangelis/utl-deduping-six-hundred-million-records-with-one-million-unique-sql-hash
https://github.com/rogerjdeangelis/utl-deleting-multiple-rows-per-subject-with-condition-hash-and-dow
https://github.com/rogerjdeangelis/utl-dosubl-persistent-hash-across-datasteps-and-procedures
https://github.com/rogerjdeangelis/utl-elegant-hash-to-add-missing-weeks-by-customer
https://github.com/rogerjdeangelis/utl-excluding-patients-that-had-same-condition-pre-and-post-clinical-randomization-hash
https://github.com/rogerjdeangelis/utl-fast-efficient-hash-to-eliminate-duplicates-in-unsorted-grouped-data
https://github.com/rogerjdeangelis/utl-fast-join-small_1g-table_with-a-moderate_50gb-tables-hash-sql
https://github.com/rogerjdeangelis/utl-fast-normalization-and-join-using-vvaluex-arrays-sql-hash-untranspose-macro
https://github.com/rogerjdeangelis/utl-hash-applying-business-rules-by-observation-when-data-and-rules-are-in-the-same-table
https://github.com/rogerjdeangelis/utl-hash-filling-in-missing-gender-for-my-patients-appointments
https://github.com/rogerjdeangelis/utl-hash-of-hashes-left-join-four-tables
https://github.com/rogerjdeangelis/utl-hash-vs-summary-min-and-max-for-four-variables-by-region-for-l89-million-obs
https://github.com/rogerjdeangelis/utl-hash_which-columns-have-duplicate-values-across-rows
https://github.com/rogerjdeangelis/utl-in-memory-hash-output-shared-with-dosubl-hash-subprocess
https://github.com/rogerjdeangelis/utl-loop-through-one-table-and-find-data-in-next-table--hash-dosubl-arts-transpose
https://github.com/rogerjdeangelis/utl-multitasking-the-hash-for-a-very-fast-distinct-ids
https://github.com/rogerjdeangelis/utl-no-need-for-sql-or-sort-merge-use-a-elegant-hash-excel-vlookup
https://github.com/rogerjdeangelis/utl-only-keep-groups-without-duplicated-accounts-hash-sql
https://github.com/rogerjdeangelis/utl-output-the-student-with-the-highest-grade-hash-defer-open
https://github.com/rogerjdeangelis/utl-remove-duplicate-words-from-a-sentence-hash-solution
https://github.com/rogerjdeangelis/utl-replicate-sets-of-rows-across-many-columns-elegant-hash
https://github.com/rogerjdeangelis/utl-sas-fcmp-hash-stored-programs-python-r-functions-to-find-common-words
https://github.com/rogerjdeangelis/utl-sharing-hash-storage-with-two-separate-datasteps-in-the-same-SAS-session
https://github.com/rogerjdeangelis/utl-simple-example-of-a-hash-of-hashes-hoh-to-split_a-table
https://github.com/rogerjdeangelis/utl-simplest-case-of-a-hash-or-sql-lookup
https://github.com/rogerjdeangelis/utl-two-table-join-benchmarks-hash-sortmerge-keyindex-and-sasfile
https://github.com/rogerjdeangelis/utl-two-techniques-for-a-persistent-hash-across-datasteps-and-procedures
https://github.com/rogerjdeangelis/utl-using-a-hash-to-compute-cumulative-sum-without-sorting
https://github.com/rogerjdeangelis/utl_benchmarks_hash_merge_of_two_un-sorted_data_sets_with_some_common_variables
https://github.com/rogerjdeangelis/utl_hash_lookup_with_multiple_keys_nice_simple_example
https://github.com/rogerjdeangelis/utl_hash_merge_of_two_un-sorted_data_sets_with_some_common_variables
https://github.com/rogerjdeangelis/utl_hash_persistent
https://github.com/rogerjdeangelis/utl_how_to_reuse_hash_table_without_reloading_sort_of
https://github.com/rogerjdeangelis/utl_many_to_many_merge_in_hash_datastep_and_sql
https://github.com/rogerjdeangelis/utl_nice_example_of_a_hash_of_hashes_by_paul_and_don
https://github.com/rogerjdeangelis/utl_nice_hash_example_of_rolling_count_of_dates_plus-minus_2_days_of_current_date
https://github.com/rogerjdeangelis/utl_select_ages_less_than_the_median_age_in_a_second_table_paul_dorfman_hash_solution
https://github.com/rogerjdeangelis/utl_simple_one_to_many_join_using_SQL_and_datastep_hashes
https://github.com/rogerjdeangelis/utl_simplified_hash_how_many_of_my_friends_are_in_next_years_math_class
https://github.com/rogerjdeangelis/utl_using_a_hash_to_transpose_and_reorder_a_table
https://github.com/rogerjdeangelis/utl_using_md5hash_to_create_checksums_for_programs_and_binary_files

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/

