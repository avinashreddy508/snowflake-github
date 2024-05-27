--create table EMP 
create or replace table emp (
     id int, 
     name string, 
     sal float);

--insert into EMP table      
insert into emp
values
    (1, 'avinash', 100),
    (2, 'sai', 400),
    (3, 'vidya', 40),
    (4, 'naveen', 6000);

--select data EMP table      
select * from  emp;

insert into emp
values
    (2, 'yojith', 100);

insert into emp
values
    (2, 'Anki reddy', 100);

update emp set name = 'haritha' where id = 6;
update emp set id = '1' where name = 'sai';

delete from emp where id = 6;

--create table emp_location 
create or replace table emp_loc (
    id int, 
    loc string);

insert into emp_loc
values
    (1, 'jersey_city'),
    (2, 'delhi'),
    (3, 'Raitran'),
    (4, 'dallas');

insert into emp_loc
values
    (2, 'vij'),
    (7, 'blore')

select * from emp_loc;

--Create Dynamic table with Lag dowstream --DT1 has downstream target lag

/*-- 
This configuration means that DT1 will not have its own independent refresh schedule but will instead refresh in response to the needs of DT2. 
It’s important to monitor the refresh history and performance to ensure that this setup meets the data freshness requirements of your applications

In Snowflake, the target lag can be set to ‘DOWNSTREAM’, which means that the dynamic table (DT1 in this case) will refresh based on the demand from the downstream tables (like DT2) that depend on it. If DT2 has a specific target lag set, and DT1 is set to ‘DOWNSTREAM’, then DT1 will refresh whenever DT2 requires it to refresh. This ensures that DT2 has the most up-to-date data from DT1.
--*/

--->Refresh lag exceeded the target lag 

CREATE
    OR REPLACE DYNAMIC TABLE EMP_DYN_TBL 
    LAG = 'downstream' 
    WAREHOUSE = COMPUTE_WH 
    AS
        select * from emp ;

--Select Dynamic table 
select * from EMP_DYN_TBL;

select * from table(information_schema.dynamic_table_refresh_history());

--Create Secure view on 
create or replace secure view emp_view 
    as
    select * from emp_dyn_tbl;

    
--Create Dynamic table with Lag 1 minute --DT2 has a target lag of 1 minutes and depends on DT1.

CREATE
    OR REPLACE DYNAMIC TABLE emp_dyn_loc_tbl 
    LAG = '1 minute' 
    WAREHOUSE = COMPUTE_WH 
AS
    select
        emp.id,
        emp.name,
        emp_loc.loc location,
        emp.sal
    from
        emp_dyn_tbl emp
        join emp_loc on emp.id = emp_loc.id;
    
select * from  emp_dyn_loc_tbl;

select * from emp_dyn_tbl;

show dynamic tables like '%dyn%';
    --EMP_DYN_TBL
    --EMP_DYN_LOC_TBL

alter dynamic table EMP_DYN_TBL suspend;
alter dynamic table EMP_DYN_LOC_TBL resume;
alter dynamic table EMP_DYN_LOC_TBL refresh;
alter dynamic table EMP_DYN_LOC_TBL set warehouse = 'compute_wh';



--Time Within Target Lag

--Refresh lag exceeded the target lag



When the refresh lag exceeds the target lag in Snowflake, it means that the dynamic table’s content is not being updated within the desired timeframe. This can happen due to various factors such as warehouse size, data size, query complexity, and similar factors1.

Here are some steps you can take to address this issue:

Review System Resources: 
	Ensure that the warehouse has sufficient resources to handle the workload. If necessary, consider scaling up the warehouse.
Optimize Queries: 
	Look into the queries used for the dynamic table and optimize them for better performance.
Adjust TARGET_LAG: 
	If the target lag is set too aggressively, it may not be realistic given the data volume and complexity. Adjust the target lag to a more achievable timeframe.
Monitor Refresh Patterns: 
	Use Snowsight or other monitoring tools to examine the refresh patterns and identify any anomalies or consistent delays.
Use Streams and Tasks: 
	If you need the dynamic table to refresh at specific times, consider using streams and tasks with a schedule timing parameter to execute the refresh at the desired times.
Manual Refresh: 
	As a last resort, you can disable the automatic refresh and create a task that executes the dynamic table refresh manually at the specific time you want.
Remember, target lag is not a guarantee but a target that Snowflake attempts to meet. It’s important to set realistic expectations for the refresh lag and to continuously monitor and adjust the system as needed1. If you continue to experience issues, it may be helpful to reach out to Snowflake support for further assistance.