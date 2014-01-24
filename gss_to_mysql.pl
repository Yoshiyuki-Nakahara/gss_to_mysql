# ------------------------------------------------------------------------- #
#                    Generate DDL/DML from GoogleSpreadSheet                #
# ------------------------------------------------------------------------- #

use strict;
use warnings;
use utf8;
use Getopt::Long qw(:config no_ignore_case);
use Data::Dumper;
use Config::YAML::Tiny;
use Net::Google::Spreadsheets;
use List::MoreUtils qw(first_value first_index);

my $ATTR_SHEET_NAME          = '属性定義';
my $LAYOUT_SHEET_NAME        = 'レイアウト';
my $TABLE_NAME_FILED_NAME    = 'テーブル名';
my $COLUMN_NAME_FILED_NAME   = 'カラム名';
my $COLUMN_DESC_FILED_NAME   = 'カラム説明';
my $DATA_TYPE_FIELD_NAME     = 'データ型';
my $DEFAULT_VALUE_FIELD_NAME = 'デフォルト値';
my $ENGINE_FIELD_NAME        = 'ENGINE';
my $CHARSET_FIELD_NAME       = 'CHARSET';

my %program;

{
    my ( $config, $option, $argv ) = parse_program_environment();
    ( $program{ 'config' },
      $program{ 'option' },
      $program{ 'argv'   } ) = ( $config, $option, $argv );

    my $sql_ddl = q{};
    my $sql_dml = q{};
    if ( $option->{'yaml'} ) {
        my $table_definition = _yaml_to_hash( $config, $option, $argv );
        $sql_ddl .= _make_sql_ddl( $config, $option, $table_definition );
    }
    elsif ( $option->{'gss-key'} ) {
        my ( $ddl_definition, $dml_definition ) = _gss_to_hash( $config, $option, $argv );
        for my $table_name ( keys %{$ddl_definition} ) {
            $sql_ddl .= _make_sql_ddl( $config, $option, $ddl_definition->{$table_name} );
        }
        if ( $dml_definition ) {
            for my $table_name ( keys %{$dml_definition} ) {
                $sql_dml .= _make_sql_dml( $config, $option, $table_name, $ddl_definition->{$table_name}, $dml_definition->{$table_name} );
            }
        }
    }

    unless ( defined $option->{'dml-only'} ) {
        print $sql_ddl;
    }
    unless ( defined $option->{'ddl-only'} ) {
        print $sql_dml;
    }
}


sub _yaml_to_hash
{
    my ( $config, $option, $argv ) = @_;

    my $filepath = $option->{'yaml'};
    my $yaml = new Config::YAML::Tiny( config => '/dev/null' );
    $yaml->read( $filepath );

    return $yaml;
}


sub _gss_to_hash
{
    my ( $config, $option, $argv ) = @_;

    my $gss_key = $option->{'gss-key'};
    # todo $0="hoge", gnome-keyring でパスワードが見えないように
    my $service = Net::Google::Spreadsheets->new(
        username => $option->{'user'},
        password => $option->{'password'},
    );
    my $spreadsheet    = $service->spreadsheet( { key => $gss_key } );
    unless ( $spreadsheet ) {
        die "Error: failed to get spreadsheet";
    }

    # find attr header row
    my $attr_worksheet = $spreadsheet->worksheet( { title => $ATTR_SHEET_NAME } );
    my @attr_rows      = $attr_worksheet->rows;
    my $attr_first_index = first_index {
        my @attr_row_tmp = values %{$_->content};
            first_index { $_ =~ /$COLUMN_NAME_FILED_NAME/   }   @attr_row_tmp >= 0
        and first_index { $_ =~ /$COLUMN_DESC_FILED_NAME/   }   @attr_row_tmp >= 0
        and first_index { $_ =~ /$DATA_TYPE_FIELD_NAME/     }   @attr_row_tmp >= 0
        and first_index { $_ =~ /$DEFAULT_VALUE_FIELD_NAME/ }   @attr_row_tmp >= 0
    } @attr_rows;
    return undef if $attr_first_index < 0;
    if ( $attr_first_index > 0 ) {
        splice @attr_rows, $attr_first_index-1, 1;
    }
    my $attr_header = shift @attr_rows;

    # get attr definition
    my %attr_definition;
    for my $attr_row ( @attr_rows ) {
        for my $attr_key ( keys %{$attr_row->content} ) {
            my $header_attr_name = $attr_header->content->{$attr_key};
            $attr_row->content->{$header_attr_name} = $attr_row->content->{$attr_key};
            delete $attr_row->content->{$attr_key};
        }
        next unless defined $attr_row->content->{$COLUMN_NAME_FILED_NAME};

        my $attr_name = $attr_row->content->{$COLUMN_NAME_FILED_NAME};
        if ( $attr_name and defined $attr_definition{$attr_name} ) {
            die 'Error : duplicate column_name $attr_name';
        }

        $attr_definition{$attr_name} = {
            'column_name'    => $attr_row->content->{$COLUMN_NAME_FILED_NAME},
            'data_type'      => $attr_row->content->{$DATA_TYPE_FIELD_NAME},
        };
        if ( defined $attr_row->content->{$DEFAULT_VALUE_FIELD_NAME} ) {
            $attr_definition{$attr_name}->{'default'} = $attr_row->content->{$DEFAULT_VALUE_FIELD_NAME};
        }
    }

    # find layout header row
    my $layout_worksheet = $spreadsheet->worksheet( { title => $LAYOUT_SHEET_NAME } );
    my @layout_rows      = $layout_worksheet->rows;
    my $layout_first_index = first_index {
        my @layout_row_tmp = values %{$_->content};
            first_index { $_ =~ /$TABLE_NAME_FILED_NAME/  } @layout_row_tmp >= 0
        and first_index { $_ =~ /$COLUMN_NAME_FILED_NAME/ } @layout_row_tmp >= 0
    } @layout_rows;
    return undef if $layout_first_index < 0;
    if ( $layout_first_index > 0 ) {
        splice @layout_rows, $layout_first_index-1, 1;
    }
    my $layout_header = shift @layout_rows;

    # get layout definition
    my %layout_definition;
    my $last_table_name;
    for my $layout_row ( @layout_rows ) {
        for my $layout_key ( keys %{$layout_row->content} ) {
            my $header_layout_name = $layout_header->content->{$layout_key};
            $layout_row->content->{$header_layout_name} = $layout_row->content->{$layout_key};
            delete $layout_row->content->{$layout_key};
        }
        next unless defined $layout_row->content->{$COLUMN_NAME_FILED_NAME};

        # get table definition
        my $table_name = $layout_row->content->{$TABLE_NAME_FILED_NAME};
        if ( $table_name ) {
            if ( defined $layout_definition{$table_name} ) {
                die 'Error : duplicate table name definition';
            }

            $layout_definition{$table_name} = {
                'table_name'    => $table_name,
                'table_option'  => {
                    'engine'  => $layout_row->content->{$ENGINE_FIELD_NAME},
                    'charset' => $layout_row->content->{$CHARSET_FIELD_NAME},
                },
            };

            $last_table_name = $table_name;
        }

        # get column definition
        my $attr_name = $layout_row->content->{$COLUMN_NAME_FILED_NAME};
        my %column_definition = %{$attr_definition{$attr_name}};
        if ( defined $layout_row->content->{'NN'} and $layout_row->content->{'NN'} eq 'Y' ) {
            $column_definition{'not_null'} = 'Yes';
        }
        if ( defined $layout_row->content->{'SQ'} and $layout_row->content->{'SQ'} eq 'Y' ) {
            $column_definition{'auto_increment'} = 'Yes';
        }
        if ( defined $layout_row->content->{'REFERENCE'} ) {
            $column_definition{'reference'} = $layout_row->content->{'REFERENCE'};
        }
        push @{$layout_definition{$last_table_name}->{column_definition}}, \%column_definition;

        # get table index definition
        if ( defined $layout_row->content->{'PK'} and $layout_row->content->{'PK'} eq 'Y' ) {
            push
                @{$layout_definition{$last_table_name}->{'_primary_key'}},
                $attr_definition{$attr_name}->{'column_name'};
        }
        if ( defined $layout_row->content->{'UQ'} ) {
            my $group_name = $layout_row->content->{'UQ'};
            push
                @{$layout_definition{$last_table_name}->{'_unique_index'}->{$group_name}},
                $attr_definition{$attr_name}->{'column_name'};
        }
        if ( defined $layout_row->content->{'IX'} ) {
            my $group_name = $layout_row->content->{'IX'};
            push
                @{$layout_definition{$last_table_name}->{'_index'}->{$group_name}},
                $attr_definition{$attr_name}->{'column_name'};
        }
    }
    # rebuild index definition
    for my $last_table_name ( keys %layout_definition ) {
        if ( defined $layout_definition{$last_table_name}->{'_primary_key'} ) {
            $layout_definition{$last_table_name}->{'primary_key'} =
                join ",", @{$layout_definition{$last_table_name}->{'_primary_key'}};
            delete $layout_definition{$last_table_name}->{'_primary_key'};
        }
        if ( defined $layout_definition{$last_table_name}->{'_unique_index'} ) {
            my $_unique_index_ref = $layout_definition{$last_table_name}->{'_unique_index'};
            for my $group_name ( keys %{$_unique_index_ref} ) {
                push
                    @{$layout_definition{$last_table_name}->{'unique_index'}},
                    join ",", @{$_unique_index_ref->{$group_name}};
            }
            delete $layout_definition{$last_table_name}->{'_unique_index'};
        }
        if ( defined $layout_definition{$last_table_name}->{'_index'} ) {
            my $_index_ref = $layout_definition{$last_table_name}->{'_index'};
            for my $group_name ( keys %{$_index_ref} ) {
                push
                    @{$layout_definition{$last_table_name}->{'index'}},
                    join ",", @{$_index_ref->{$group_name}};
            }
            delete $layout_definition{$last_table_name}->{'_index'};
        }
    }
    verbose( Dumper \%layout_definition, 1 );

    # DML
    my %dml_definition;
    for my $table_name ( keys %layout_definition ) {
        my $dml_worksheet = $spreadsheet->worksheet( { 'title' => $table_name } );
        next unless $dml_worksheet;

        # find dml header row
        my @dml_rows = $dml_worksheet->rows;
        my $column_count = scalar @{$layout_definition{$table_name}->{column_definition}};
        my $dml_first_index = first_index {
            my @dml_row_tmp = values %{$_->content};
            my $hits = 0;
            for my $column_definition ( @{$layout_definition{$table_name}->{column_definition}} ) {
                my $column_name = $column_definition->{column_name};
                if ( first_index { $_ =~ /$column_name/ } @dml_row_tmp >= 0 ) {
                    $hits++;
                }
            }

            $hits >= $column_count;
        } @dml_rows;
        return undef if $dml_first_index < 0;
        if ( $dml_first_index > 0 ) {
            splice @dml_rows, $dml_first_index-1, 1;
        }
        my $dml_header = shift @dml_rows;

        # get dml definition
        for my $dml_row ( @dml_rows ) {
            for my $dml_key ( keys %{$dml_row->content} ) {
                my $header_dml_name = $dml_header->content->{$dml_key};
                $dml_row->content->{$header_dml_name} = $dml_row->content->{$dml_key};
                delete $dml_row->content->{$dml_key};
            }

            my %dml_tmp;
            for my $column_key ( keys %{$dml_header->content} ) {
                my $column_name = $dml_header->content->{$column_key};
                if ( defined $dml_row->content->{$column_name} ) {
                    $dml_tmp{$column_name} = $dml_row->content->{$column_name};
                }
            }
            if ( scalar keys %dml_tmp ) {
                push @{$dml_definition{$table_name}}, \%dml_tmp;
            }
        }
    }

    return ( \%layout_definition, \%dml_definition );
}


sub _make_sql_ddl
{
    my ( $config, $option, $hash ) = @_;

    verbose( "[ _make_sql_ddl ]" );
    verbose( Dumper $hash );

    unless ( defined $hash->{table_name} and $hash->{table_name} ) {
        die 'Error: undefined table_name';
    }
    my $table_name = $hash->{table_name};

    if ( defined $option->{table} && $option->{table} ne $table_name ) {
        return "";
    }

    my $temporary = '';
    if ( defined $hash->{'temporary'} && $hash->{'temporary'} eq 'Yes' ) {
        $temporary = 'TEMPORARY';
    }
    my $if_not_exists = 'IF NOT EXISTS';
    if ( defined $hash->{'temporary'} && $hash->{'temporary'} eq 'No' ) {
        $if_not_exists = '';
    }
    my $create_sql = sprintf "CREATE %s TABLE %s %s",
                            ( $temporary, $if_not_exists, $table_name );

    my @create_definition_sql;
    for my $column ( @{$hash->{column_definition}} ) {
        no warnings 'uninitialized';

        my $not_null = q{};
        if ( defined $column->{not_null} and $column->{not_null} eq 'Yes' ) {
            $not_null = 'NOT NULL';
        }
        my $auto_increment = q{};
        if ( defined $column->{auto_increment} and $column->{auto_increment} eq 'Yes' ) {
            $auto_increment = 'AUTO_INCREMENT';
        }
        my $default = q{};
        if ( defined $column->{default} ) {
            # can not be specified at the same time default and auto_increment
            unless ( $auto_increment ) {
                $default = 'DEFAULT ' .  $column->{default};
            }
        }
        my $sql = sprintf "%s %s %s %s %s %s",
            ( $column->{column_name}, $column->{data_type}, $not_null,
            $default, $auto_increment, $column->{reference} );
        push @create_definition_sql, $sql;
    }
    if ( defined $hash->{primary_key} and $hash->{primary_key} ) {
        my $primary_key = $hash->{primary_key};
        push @create_definition_sql, sprintf "PRIMARY KEY (%s)", ( $primary_key );
    }
    if ( defined $hash->{unique_index} and scalar @{$hash->{unique_index}} ) {
        for my $unique_index ( @{$hash->{unique_index}} ) {
            push @create_definition_sql, sprintf "UNIQUE INDEX (%s)", ( $unique_index );
        }
    }
    if ( defined $hash->{index} and scalar @{$hash->{index}} ) {
        for my $index ( @{$hash->{index}} ) {
            push @create_definition_sql, sprintf "INDEX (%s)", ( $index );
        }
    }

    my @table_option;
    if ( defined $hash->{table_option}->{engine} and $hash->{table_option}->{engine} ) {
        my $engine = $hash->{table_option}->{engine};
        push @table_option, sprintf "ENGINE=%s ", ( $engine );
    }
    if ( defined $hash->{table_option}->{charset} and $hash->{table_option}->{charset} ) {
        my $charset = $hash->{table_option}->{charset};
        push @table_option, sprintf "CHARSET=%s ", ( $charset );
    }

    my $sql = "--\n";
    if ( $option->{'drop-table'} ) {
        $sql .= "DROP TABLE IF EXISTS $table_name;\n";
    }
    $sql .= sprintf "%s\n(\n    %s\n)\n%s\n;\n",
        (
            $create_sql,
            ( join ",\n    ", @create_definition_sql ),
            ( join ' ', @table_option ),
        );

    return $sql;
}


sub _make_sql_dml
{
    my ( $config, $option, $table_name, $table_definition, $rows ) = @_;

    verbose( "[ _make_sql_dml ]" );
    verbose( Dumper $rows );

    if ( defined $option->{table} && $option->{table} ne $table_name ) {
        return "";
    }

    my $sql = "-- [DML $table_name]\n";
    if ( defined $table_definition->{primary_key} ) {
        $sql .= "--   primary key -> $table_definition->{primary_key}\n";
    }
    for my $column_name ( @{$table_definition->{unique_index}} ) {
        $sql .= "--   unique index -> $column_name\n";
    }

    if ( $option->{'dml-transaction'} ) {
        $sql .= "START TRANSACTION;\n";
    }

    for my $row ( @{$rows} ) {
        my @cols_dml;
        for my $column_name ( keys %{$row} ) {
            my $sql_tmp;
            for my $column_definition ( @{$table_definition->{column_definition}} ) {
                if ( $column_definition->{column_name} eq $column_name ) {
                    if ( $column_definition->{data_type} =~ /int/i ) {
                        $sql_tmp = "$column_name = $row->{$column_name}";
                    }
                    else {
                        $sql_tmp = "$column_name = '$row->{$column_name}'";
                    }
                    last;
                }
            }
            push @cols_dml, $sql_tmp;
        }
        $sql .= "REPLACE INTO $table_name SET ";
        $sql .= join ", ", @cols_dml;
        $sql .= ";\n";
    }

    if ( $option->{'dml-transaction'} ) {
        $sql .= "COMMIT;\n";
    }

    return $sql;
}


sub empty
{
    my ( $val_ref ) = @_;

    #return 1 unless defined $val_ref;
    unless ( my $ref = ref $val_ref ) {
        return $val_ref ? 0 : 1;
    }
    elsif ( $ref eq 'SCALAR' ) {
        return $$val_ref ? 0 : 1;
    }
    elsif ( $ref eq 'ARRAY' ) {
        return scalar @{$val_ref} ? 0 : 1;
    }
    elsif ( $ref eq 'HASH' ) {
        return scalar keys %{$val_ref} ? 0 : 1;
    }

    return undef;
}


sub usage
{
    my $usage = $_[0] ? "$_[0]\n" : "";

    my $basename = $0;
    $basename =~ s{\.pl}{}xms;

    return $usage . <<"END_USAGE"
usage: perl $basename.pl options
  options: -h|--help    : print usage and exit
           -v|--verbose : print message verbosely
           -c|--config  : specify config file

           --yaml       : yaml filepath for table definition
           --gss-key    : GoogleSpreadSheet key
           --user       : google account user     with --gss option
           --password   : google account password with --gss option

           --ddl-only   : output ddl only
           --dml-only   : output dml only

           --table      : filtering table name (default: all)
           --drop-table : drop table if exists table_name
           --dml-transaction : add START TRANSACTION; ... COMMIT;
END_USAGE
}


sub verbose
{
    my ( $str, $level, $nl ) = @_;

    $level = 1 unless $level;
    $nl = "\n" unless $nl;

    local $| = 1;
    if ( defined $program{'option'}->{verbose} ) {
        print "$str$nl" if $program{'option'}->{verbose} >= $level;
    }
}


sub parse_program_environment
{
    my $option = parse_program_option();
    my $config = parse_program_config( $option->{config} );
    my @argv   = parse_program_argv( $config, $option );

    return ( $config, $option, \@argv );
}


sub parse_program_config
{
    my ( $filename ) = @_;
    verbose( "[ parsing program config file ] ...", 2 );

    my $config = new Config::YAML::Tiny( config => '/dev/null' );
    unless ( $filename ) {
        # try path that is changed to .conf extension .pl
        my $conf_path = __FILE__;
        $conf_path =~ s/([^\.]+?)$/conf/;
        if ( -s $conf_path ) {
            verbose( "  filename => $conf_path", 2 );
            $config->read( $conf_path );
        }
    }
    else {
        verbose( "  filename => $filename", 2 );
        if ( ! -s $filename ) {
            die "error: invaild config file path $filename";
        }
        $config->read( $filename );
    }

    return $config;
}


sub parse_program_option
{
    my $option = new Config::YAML::Tiny( config => '/dev/null' );
    GetOptions(
        $option,

        'help',               # print help and exit
        'verbose+',           # print message verbosely
        'config=s',           # specify config file

        'yaml=s',             # yaml filepath for table definition
        'gss-key=s',          # GoogleSpreadSheet key
        'user=s',             # google account user     with --gss-key option
        'password=s',         # google account password with --gss-key option

        'ddl-only+',          # output ddl only
        'dml-only+',          # output dml only

        'table=s',            # specify table name (default: all)
        'drop-table+',        # drop table if exists table_name
        'dml-transaction+',   # add START TRANSACTION; ... COMMIT;
    ) or die usage;

    verbose( "[ parsing get program option(s) ] ...", 2 );
    $program{'option'} = $option;

    print usage() and exit if $option->{help};

    unless ( $option->{'yaml'} || $option->{'gss-key'} ) {
        die 'must be specified --yaml or --gss-key';
    }
    if ( defined $option->{'gss-key'} ) {
        unless ( defined $option->{'user'} && defined $option->{'password'} ) {
            die 'must be specified --user and --password';
        }
    }

    return $option;
}


sub parse_program_argv
{
    my ( $config, $option ) = @_;
    verbose( "[ parsing program argv(s) ] ...", 2 );

    my @argv = @ARGV;
    for my $arg ( @argv ) {
        verbose( "  argv => $arg", 2 );
    }
    die usage() if @argv != 0;

    return @argv;
}


__END__

