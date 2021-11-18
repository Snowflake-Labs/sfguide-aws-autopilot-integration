--
-- Step 1: Data Engineering (import dataset and simple data prep)
--
use role autopilot_role;
use database autopilot_db;
use warehouse autopilot_wh;
use schema demo;

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
) ;
create file format cc_file_format type=csv  skip_header=1;

copy into cc_dataset from @~/autopilot file_format=cc_file_format;

select * from cc_dataset;

select isfraud, count(*) from cc_dataset group by isfraud;

create or replace table cc_dataset_train as
   select * 
   from cc_dataset sample (50) seed (1000);
   
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
  'cc-fraud-prediction-dev'  -- model name
  ,'cc_dataset_train'        -- training data location
  ,'isfraud'                 -- target column
  ,null                      -- objective metric
  ,null                      -- problem type
  ,5                         -- number of candidates to be evaluated
                             --    via hyperparameter tuning 
  ,15*60*60                  -- training timeout
  ,'True'                    -- create scoring endpoint yes/no
  ,1*60*60                   -- endpoint TTL
) output;

select aws_autopilot_describe_model('cc-fraud-prediction-dev') output;


--
-- Step 3 : Score test dataset and evaluate model
--

select aws_autopilot_describe_endpoint('cc-fraud-prediction-dev') output;

-- recreate endpoint if it has expired
select aws_autopilot_create_endpoint (
    'cc-fraud-prediction-dev' 
    ,'cc-fraud-prediction-dev-m5-4xl-2' 
    ,1*60*60) output;

create or replace table cc_dataset_prediction_result as 
  select isfraud,(parse_json(
      aws_autopilot_predict_outcome(
        'cc-fraud-prediction-dev'
        ,array_construct(
           step,type,amount,nameorig,oldbalanceorg,newbalanceorig
           ,namedest,oldbalancedest,newbalancedest,isflaggedfraud))
    ):"predicted_label")::varchar predicted_label
  from cc_dataset_train;
   
select isfraud, predicted_label, count(*)
from cc_dataset_prediction_result
group by isfraud, predicted_label
order by isfraud, predicted_label;

select 'overall' predicted_label
        ,sum(iff(isfraud = predicted_label,1,0)) correct_predictions
        ,count(*) total_predictions
        ,(correct_predictions/total_predictions)*100 accuracy
from cc_dataset_prediction_result;

select predicted_label
        ,sum(iff(isfraud = predicted_label,1,0)) correct_predictions
        ,count(*) total_predictions
        ,(correct_predictions/total_predictions)*100 accuracy
from cc_dataset_prediction_result
group by predicted_label;

--
-- step 4: Optimize Model 
--

select aws_autopilot_create_model (
  'cc-fraud-prediction-prd' -- model name
  ,'cc_dataset_train'       -- training data table name
  ,'isfraud'                -- target column
) output;
    
select aws_autopilot_describe_model('cc-fraud-prediction-prd') output;
    
create or replace table cc_dataset_prediction_result_prd as 
  select isfraud,(parse_json(
      aws_autopilot_predict_outcome(
        'cc-fraud-prediction-prd'
        ,array_construct(
           step,type,amount,nameorig,oldbalanceorg,newbalanceorig
           ,namedest,oldbalancedest,newbalancedest,isflaggedfraud))
    ):"predicted_label")::varchar predicted_label
  from cc_dataset_train; 
  
select isfraud, predicted_label, count(*)
from cc_dataset_prediction_result_prd
group by isfraud, predicted_label
order by isfraud, predicted_label;

select 'overall' predicted_label
        ,sum(iff(isfraud = predicted_label,1,0)) correct_predictions
        ,count(*) total_predictions
        ,(correct_predictions/total_predictions)*100 accuracy
from cc_dataset_prediction_result_prd;

select predicted_label
        ,sum(iff(isfraud = predicted_label,1,0)) correct_predictions
        ,count(*) total_predictions
        ,(correct_predictions/total_predictions)*100 accuracy
from cc_dataset_prediction_result_prd
group by predicted_label;
