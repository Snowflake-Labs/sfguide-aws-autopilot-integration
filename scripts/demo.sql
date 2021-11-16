--
-- Step 1: Data Engineering (import dataset and simple data prep)
--
use role autopilot_role;
use database autopilot_db;
use warehouse autopilot_wh;

create or replace table cc_dataset (
	step number(38,0),
	type varchar(16777216),
	amount number(38,2),
	nameorig varchar(16777216),
	oldbalanceorg number(38,2),
	newbalanceorig number(38,2),
	namedest varchar(16777216),
	oldbalancedest number(38,2),
	newbalancedest number(38,2),
	isfraud boolean,
	isflaggedfraud boolean
);

create file format demo type=csv  skip_header=1;

-- 
-- Upload the dataa files to @~/autopilot_demo/data
--
-- copy into cc_dataset from @~/autopilot_demo/data file_format=demo;

select * from cc_dataset;

select isfraud, count(*) from cc_dataset group by isfraud;

create or replace table cc_dataset_train as
   select * from cc_dataset sample (50);

create or replace table cc_dataset_test as
   (select * from cc_dataset ) minus 
   (select * from cc_dataset_train) ;

(select 'cc_dataset_train' name, isfraud, count(*) 
 from cc_dataset_train group by isfraud) 
union
(select 'cc_dataset_test' name, isfraud, count(*) 
 from cc_dataset_test group by isfraud)
order by name, isfraud;

--
-- Step 2: Build initial Model
--
   
select aws_autopilot_create_model (
  'cc-fraud-prediction  '   -- model name
  ,'cc_dataset_train'       -- training data table name
  ,'isfraud'                -- target column
  ,null                     -- objective metric
  ,null                     -- problem type
  ,5                        -- number of candidates for hyper parameter tuning
  ,15*60*60                 -- training timeout
  ,'True'                   -- create scoring endpoint yes/no
  ,1*60*60                  -- endpoint idle timeout
);

select aws_autopilot_describe_model('cc-fraud-prediction'); 

--
-- Step 3 : Score test dataset and evaluate model
--

select aws_autopilot_describe_endpoint('cc-fraud-prediction');

-- re-create endpoint in case it has expired
select aws_autopilot_create_endpoint (
    'cc-fraud-prediction' 
    ,'cc-fraud-prediction-m5-4xl-2' 
    ,1*60*60);


create or replace table cc_dataset_prediction_result as 
  select isfraud,(parse_json(
      aws_autopilot_predict_outcome(
        'cc-fraud-prediction'
        ,array_construct(
           step,type,amount,nameorig,oldbalanceorg,newbalanceorig
           ,namedest,oldbalancedest,newbalancedest,isflaggedfraud))
    ):"predicted_label")::varchar predicted_label
  from cc_dataset_train;
   
select isfraud, predicted_label, count(*)
from cc_dataset_prediction_result
group by isfraud, predicted_label
order by isfraud, predicted_label;

--
-- Optimize Model 
--

select aws_autopilot_create_model (
  'cc-fraud-prediction-final' -- model name
  ,'cc_dataset_train'         -- training data table name
  ,'isfraud'                  -- target column
);

select aws_autopilot_describe_model('cc-fraud-prediction-final'); 

select aws_autopilot_describe_endpoint('cc-fraud-prediction-final');

-- re-create endpoint in case it has expired
select aws_autopilot_create_endpoint (
    'cc-fraud-prediction-final' 
    ,'cc-fraud-prediction-final-m5-4xl-2' 
    ,1*60*60);


create or replace table cc_dataset_prediction_result_final as 
  select isfraud,(parse_json(
      aws_autopilot_predict_outcome(
        'cc-fraud-prediction-final'
        ,array_construct(
           step,type,amount,nameorig,oldbalanceorg,newbalanceorig
           ,namedest,oldbalancedest,newbalancedest,isflaggedfraud))
    ):"predicted_label")::varchar predicted_label
  from cc_dataset_train;

select isfraud, predicted_label, count(*)
from cc_dataset_prediction_result_final
group by isfraud, predicted_label
order by isfraud, predicted_label;


