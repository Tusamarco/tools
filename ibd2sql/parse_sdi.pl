#!/usr/bin/perl
# This tool is "fat-packed": most of its dependent modules are embedded
# in this file.
#######################################
#
# SDI Jason to SQL  v1 
#
# Author Marco Tusa 
# Copyright (C) (2016 - 2019)
# 
#
#THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
#WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
#MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
#This program is free software; you can redistribute it and/or modify it under
#the terms of the GNU General Public License as published by the Free Software
#Foundation, version 2; OR the Perl Artistic License.  On UNIX and similar
#systems, you can issue `man perlgpl' or `man perlartistic' to read these
#licenses.
#
#You should have received a copy of the GNU General Public License along with
#this program; if not, write to the Free Software Foundation, Inc., 59 Temple
#Place, Suite 330, Boston, MA  02111-1307  USA.

#######################################
package sdi_2_sql ;
use Time::HiRes qw(gettimeofday);
use strict;
use Getopt::Long;
use IO::Compress::Gzip qw(gzip $GzipError) ;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError) ;
use Pod::Usage;
use JSON::PP qw(encode_json decode_json from_json to_json);

$Getopt::Long::ignorecase = 0;
my $Param = {};
my $help = '';
my $host = '' ;
my $debug = 0 ;

my $incomingJSON;
my $outgoingSQL;

sub URLDecode {
    my $theURL = $_[0];
    $theURL =~ tr/+/ /;
    $theURL =~ s/%([a-fA-F0-9]{2,2})/chr(hex($1))/eg;
    $theURL =~ s/<!--(.|\n)*-->//g;
    return $theURL;
}
sub URLEncode {
    my $theURL = $_[0];
   $theURL =~ s/([\W])/"%" . uc(sprintf("%2.2x",ord($1)))/eg;
   return $theURL;
}



sub main(){
    # ============================================================================
    #+++++ INITIALIZATION
    # ============================================================================
    
    if($#ARGV < 0){
        pod2usage(-verbose => 2) ;
       	exit 1;
    }
    
    $Param->{log}       = undef ;
    $Param->{debug}      = 0; 
    $Param->{directory_in} = undef;
    $Param->{directory_out} = undef;
    $Param->{recursive} = 0;
    $Param->{print_execution} = 1;
    $Param->{one_file} = 0;
    $Param->{development} = 1;
    
    my $run_pid_dir = "/tmp" ;
    
    #if (
    GetOptions(
        'debug|d:i'      => \$Param->{debug},
        'log:s'      => \$Param->{log},
        'directory_in|i:s'=> \$Param->{directory_in},
        'directory_out|o:s'=> \$Param->{directory_out},
        'recursive|r:i' =>	\$Param->{recursive},
        'print_execution|p:i' =>	\$Param->{print_execution},
        'one_file:i' => \$Param->{one_file},
        'help|?'       => \$Param->{help}
       
    ) or pod2usage(2);
    pod2usage(-verbose => 2) if $Param->{help};

    die print Utils->print_log(1,"Option --directory_in not specified, and it is mandatory.\n") unless defined($Param->{directory_in});
    die print Utils->print_log(1,"Option --directory_out not specified, and it is mandatory.\n") unless defined($Param->{directory_out});

    #============================================================================
    # Execution
    #============================================================================
    if(defined $Param->{log}){
       open(FH, '>', $Param->{log}.".log") or die Utils->print_log(1,"cannot open file");
       FH->autoflush if $Param->{development} < 0;
       select FH;
    }
    
    sub read_incoming_dir(){
        #Generate an Array of SDI files
        
        #create Sdi object and convert to SQL
        #Memory wise makes more sense to parse and write one by one
        
        
        #For each file generate table->attribute->Index->Partition    
    }
    
    
    
}

package Utils;
use Time::HiRes qw(gettimeofday);
    #Print time from invocation with milliseconds
    sub get_current_time{
        use POSIX qw(strftime);
        my $t = gettimeofday();
        my $date = strftime "%Y/%m/%d %H:%M:%S", localtime $t;
        $date .= sprintf ".%03d", ($t-int($t))*1000; # without rounding
        
        return $date;
    }

    #Print a log entry
    sub print_log($$){
        my $log_level = $_[1];
        my $text = $_[2];
        my $log_text = "[ - ] ";
	
        SWITCH: {
              if ($log_level == 1) { $log_text= "[ERROR] "; last SWITCH; }
              if ($log_level == 2) { $log_text= "[WARN] "; last SWITCH; }
              if ($log_level == 3) { $log_text= "[INFO] "; last SWITCH; }
              if ($log_level == 4) { $log_text= "[DEBUG] "; last SWITCH; }
       }
       return Utils::get_current_time.":".$log_text.$text;	
    }
    
        
    
    #trim a string
    sub  trim {
        my $s = shift;
        $s =~ s/^\s+|\s+$//g;
        return $s
    };
{
{
package Sdi  ;  
use Time::HiRes qw(gettimeofday);
my $json_text=undef;
my $file_name = undef;
my $full_path = undef;
  sub new {
        my $class = shift;
        my $self = {
            _json_text => undef,
            _file_name  => 0,
            _full_path  => undef
        };
        bless $self, $class;
        return $self;

        
    sub full_path{
        my ( $self, $in ) = @_;
        $self->{_full_path} = $in if defined($in);
        return $self->{_full_path};
    }
        
    sub file_name{
        my ( $self, $in ) = @_;
        $self->{_file_name} = $in if defined($in);
        return $self->{_file_name};
    }

    sub json_text{
        my ( $self, $in ) = @_;
        $self->{_json_text} = $in if defined($in);
        return $self->{_json_text};
    }
     
        
   sub read_file(){
        
    }
    

}   
package Partition;
use Time::HiRes qw(gettimeofday);
my $name = undef;
my $order_number=0;
my $description_value=undef;
my $table_space=undef;

  sub new {
        my $class = shift;
        my $self = {
            _name => undef,
            _order_number  => 0,
            _description_value  => undef,
            _table_space => undef
        };
        bless $self, $class;
        return $self;
        
    }
    sub table_space{
        my ( $self, $in ) = @_;
        $self->{_table_space} = $in if defined($in);
        return $self->{_table_space};
    }
  
    sub description_value{
        my ( $self, $in ) = @_;
        $self->{_description_value} = $in if defined($in);
        return $self->{_description_value};
    }
    
    sub order_number{
        my ( $self, $in ) = @_;
        $self->{_order_number} = $in if defined($in);
        return $self->{_order_number};
    }
    
    sub name{
        my ( $self, $in ) = @_;
        $self->{_name} = $in if defined($in);
        return $self->{_name};
    }

           #"name": "asia",
           # "parent_partition_id": 18446744073709552000,
           # "number": 0,
           # "se_private_id": 1841,
           # "description_utf8": "101",
           # "engine": "InnoDB",
           # "comment": "",
           # "options": "",
           # "se_private_data": "autoinc=0;version=0;",
           # "values": [
           #   {
           #     "max_value": false,
           #     "null_value": false,
           #     "list_num": 0,
           #     "column_num": 0,
           #     "value_utf8": "101"
           #   }
}    
    
{    
package Table;
use Time::HiRes qw(gettimeofday);
my $name =undef;
my $is_partitioned = 0;
my %indexes =undef;
my %attributes=undef;
my $engine=undef;
my $charset=undef;
my $collation=undef
my $schema=undef;
my $tablespace=undef;
my %partitions=undef;
my $partition_definition=undef;

  sub new {
        my $class = shift;
        my $self = {
            _name => undef,
            _is_partitioned  => 0,
            _indexes  => undef,
            _attributes => undef,
            _engine => undef,
            _charset => undef,
            _collation => undef,
            _schema => undef,
            _tablespace => undef,
            _partitions => undef,
            _partition_definition =>undef
        };
        bless $self, $class;
        return $self;
        
    }
    sub partition_definition{
        my ( $self, $in ) = @_;
        $self->{_partition_definition} = $in if defined($in);
        return $self->{_partition_definition};
    }
  
    sub partitions{
        my ( $self, $in ) = @_;
        $self->{_partitions} = $in if defined($in);
        return $self->{_partitions};
    }
  
    sub tablespace{
        my ( $self, $in ) = @_;
        $self->{_tablespace} = $in if defined($in);
        return $self->{_tablespace};
    }
 
    sub schema{
        my ( $self, $in ) = @_;
        $self->{_schema} = $in if defined($in);
        return $self->{_schema};
    }

    sub collation{
        my ( $self, $in ) = @_;
        $self->{_collation} = $in if defined($in);
        return $self->{_collation};
    }
  
    sub charset{
        my ( $self, $in ) = @_;
        $self->{_charset} = $in if defined($in);
        return $self->{_charset};
    }

    sub engine{
        my ( $self, $in ) = @_;
        $self->{_engine} = $in if defined($in);
        return $self->{_engine};
    }

    sub attributes{
        my ( $self, $in ) = @_;
        $self->{_attributes} = $in if defined($in);
        return $self->{_attributes};
    }

    
    sub is_partitioned{
        my ( $self, $in ) = @_;
        $self->{_is_partitioned} = $in if defined($in);
        return $self->{_is_partitioned};
    }

    sub indexes{
        my ( $self, $in ) = @_;
        $self->{_indexes} = $in if defined($in);
        return $self->{_indexes};
    }
    sub name{
        my ( $self, $in ) = @_;
        $self->{_name} = $in if defined($in);
        return $self->{_name};
    }



}

{    
package Index;
use Time::HiRes qw(gettimeofday);
my $index_name=undef;
my $index_position=0;
my %attributes=undef;

  sub new {
        my $class = shift;
        my $self = {
            _index_name  => 0,
            _index_position  => 0,
            _attributes => undef
        };
        bless $self, $class;
        return $self;
        
    }
    sub index_position{
        my ( $self, $in ) = @_;
        $self->{_index_position} = $in if defined($in);
        return $self->{_index_position};
    }
  
    sub index_name{
        my ( $self, $in ) = @_;
        $self->{_index_name} = $in if defined($in);
        return $self->{_index_name};
    }

    sub attributes{
        my ( $self, $in ) = @_;
        $self->{_attributes} = $in if defined($in);
        return $self->{_attributes};
    }


}

{
package Attribute;
use Time::HiRes qw(gettimeofday);
my $name=undef;
my $order=0;
my $createText=undef;
my $default_value=undef;
my $is_nullable=0;
my $is_zerofill=0;
my $is_unsigned=0;
my $is_auto_increment=0;

  sub new {
        my $class = shift;
        my $self = {
            _name  => undef,
            _order  => 0,
            _createText => undef,
            _default_value => undef,
            _is_nullable => 0,
            _is_zerofill => 0,
            _is_unsigned => 0,
            _is_auto_increment => 0                 
        };
        bless $self, $class;
        return $self;
        
    }
    sub is_auto_increment{
        my ( $self, $in ) = @_;
        $self->{_is_auto_increment} = $in if defined($in);
        return $self->{_is_auto_increment};
    }

    sub is_unsigned{
        my ( $self, $in ) = @_;
        $self->{_is_unsigned} = $in if defined($in);
        return $self->{_is_unsigned};
    }
  
    sub is_zerofill{
        my ( $self, $in ) = @_;
        $self->{_is_zerofill} = $in if defined($in);
        return $self->{_is_zerofill};
    }

    sub is_nullable{
        my ( $self, $in ) = @_;
        $self->{_is_nullable} = $in if defined($in);
        return $self->{_is_nullable};
    }
  
    sub default_value{
        my ( $self, $in ) = @_;
        $self->{_default_value} = $in if defined($in);
        return $self->{_default_value};
    }
  
    sub createText{
        my ( $self, $in ) = @_;
        $self->{_createText} = $in if defined($in);
        return $self->{_createText};
    }

    sub order{
        my ( $self, $in ) = @_;
        $self->{_order} = $in if defined($in);
        return $self->{_order};
    }
  
    sub name{
        my ( $self, $in ) = @_;
        $self->{_name} = $in if defined($in);
        return $self->{_name};
    }



}
