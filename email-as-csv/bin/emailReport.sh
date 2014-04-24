#!/bin/bash -e
if [ $# -lt 1  ];
then
  echo "Usage: `basename $0` report_name"
  exit 
fi
start_time=$(date +"%s")
BASEDIR=$(dirname $0)
report_name=$1
config_file=$BASEDIR/../conf/$report_name.properties
source $config_file
log_file=$log_dir/$report_name.log
tmp_dir=$report_dir/$report_name
output_csv_data=$tmp_dir/$report_name.dat
output_csv_file=$tmp_dir/$report_name.csv
output_csv_zip=$tmp_dir/$report_name.csv.zip
output_message=$tmp_dir/$report_name.msg
echo "Report Name: $report_name" >> $log_file 
if [ -d $tmp_dir ]; then
  rm -rf $tmp_dir
  echo "$tmp_dir is removed." >> $log_file 
fi
hive_query="set mapred.job.name = $report_name; set hive.exec.reducers.max = 8; insert overwrite local directory '$tmp_dir' "$report_query"; exit;"
echo $hive_query | hive  -hiveconf pool.name=digestion 2>&1 | tee -a $log_file
if [ $? -ne 0 ];
then
        echo "Error with return code $rc while executing hive query: $hive_query" >> $log_file
        echo $email_message_failed > $output_message
        /usr/bin/mutt -n -s $email_subject $email_address  < $output_message
        exit 2
fi
if [ "$(ls -A $tmp_dir)" ];  
then 
    echo "Process output for $report_name"
    cat $tmp_dir/* > $output_csv_data
    echo $report_header > $output_csv_file
    sed -i 's/"/""/g;s/^/"/;s/$/"/;s//","/g;s/\\N/NA/g' $output_csv_data
    cat $output_csv_data >> $output_csv_file 
    email_attachment=$output_csv_file
    if [ $compress_report = 'true' ];
    then 
      /usr/bin/zip -j $output_csv_zip $output_csv_file
      email_attachment=$output_csv_zip
    fi 
    echo $email_message_success > $output_message
    end_time=$(date +"%s")
    minutes=$(( ($end_time - $start_time) / 60 ))
    /usr/bin/mutt -c $email_cc_address -s "$report_name succeeded in $minutes MIN" -a $email_attachment  -- $email_to_address  < $output_message  
    exit 0
else
    echo "No output  for $report_name" >> $log_file
    exit 0
fi
