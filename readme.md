
## gss_to_mysql

Generate DDL/DML for MySQL from GoogleSpreadSheet

## Usage

    perl gss_to_mysql.pl [options]
    options: -h|--help    : print usage and exit
             -v|--verbose : print message verbosely

             --gss-title  : GoogleSpreadSheet Title
             --user       : google account user     with --gss-key option
             --password   : google account password with --gss-key option

             --ddl-only   : output ddl only
             --dml-only   : output dml only

             --table      : filtering table name (default: all)
             --drop-table : drop table if exists table_name
             --dml-transaction : add START TRANSACTION; ... COMMIT;

## Example

    [Sample GoogleSpreadSheet, sorry, japanese only]
    https://docs.google.com/spreadsheet/ccc?key=0AkNwR6_Dui92dG14MG03cklKSEdOYV9hVldFdFBEN1E

    [Command]
    perl gss_to_mysql.pl \
        --gss-key=0AkNwR6_Dui92dG14MG03cklKSEdOYV9hVldFdFBEN1E \
        --user=<your google acount username> \
        --password=<your google account password>

## License

Apache License, Version 2.0

## Copyright

Copyright (c) 2014 Yoshiyuki Nakahara

