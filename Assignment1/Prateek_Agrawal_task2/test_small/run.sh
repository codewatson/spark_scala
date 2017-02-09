spark-submit --class sort_tag_rat --master local[4] Prateek_Agrawal_task2.jar 
rm -rf spark-warehouse
cp tags_avg.csv/* .
rm -rf tags_avg.csv/
rm _SUCCESS
rm .*
