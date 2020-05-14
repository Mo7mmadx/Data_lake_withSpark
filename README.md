
    
<h2>
 Datasets I have used  :
</h2>

* 1- Song data: ```s3://udacity-dend/song_data```
* 2- Log data:  ```s3://udacity-dend/log_data ```
* Log data json path:  ```s3://udacity-dend/log_json_path.json ```




<h1></h1>

<h2> Program execution : </h2>

First you should run ```etl.py```  on Termanl to connect to aws  cluster and the scrept will reade from ```s3://udacity-dend/song_dat``` and ```s3://udacity-dend/log_data```  then wrate on my S3 cluster and save the tables . 


<h1></h1>
<h2> Schema Design :  </h2>

Please fine this digram I have created to descibe the schema .

![GitHub Logo](ER1.png)

<h1></h1>


<h2>The Purpose of this Project: </h2>

Building an ETL pipeline that extracts data from udacity S3, Proccesing them using pyspark them in EMR cluster, and  store data into a set of dimensional tables for analytics team to continue finding insights in what songs their users are listening to in my S3 . 

<h1></h1>



