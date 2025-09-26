CREATE OR REPLACE PROCEDURE EXT.SP_RPT_YTDATTAINMENT_DETAIL(OUT FILENAME VARCHAR(100),
pprocessingunitname VARCHAR(100),
pperiodname VARCHAR(100)) LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS V_PERIODNAME varchar2(255);

V_PERIODNAME_NOSPACE varchar2(255);
V_CALENDARNAME varchar2(255) := 'LITE Fiscal Calendar';
V_PERIODSEQ number;
V_CALENDARSEQ number;
V_PUSEQ number ;
V_STARTDATE DATE;
V_ENDDATE DATE;
V_FYSTARTPERIODSEQ number;

c_eot DATE := to_date('01012200','MMDDYYYY') ;

BEGIN
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_YTDATTAINMENT_DETAIL Exception Occurred - ' ||::SQL_ERROR_CODE || ' - ' ||substr(::SQL_ERROR_MESSAGE,100) ,
	99
FROM
	dummy ;

INSERT 	INTO 	EXT.RPT_DEBUG
SELECT 	'0046', 	NOW(),	'SP_RPT_YTDATTAINMENT_DETAIL procedure started', 	1 FROM 	dummy ;

--CALL "EXT"."SP_RPT_ORGDATA" (pprocessingunitname ,pperiodname);

SELECT
	'0046',
	NOW(),
	'SP_RPT_YTDATTAINMENT_DETAIL Proc Intialized',
	1.1
FROM
	dummy ;

SELECT 	processingunitseq 
INTO 	V_PUSEQ
FROM 	tcmp.cs_processingunit
WHERE	name = pprocessingunitname;

SELECT 	calendarseq
INTO 	V_CALENDARSEQ
FROM	tcmp.cs_calendar
WHERE 	removedate = c_eot
AND name = V_CALENDARNAME;


SELECT
	periodseq,
	NAME,
	startdate,
	enddate,
	calendarseq
INTO
	V_PERIODSEQ,
	V_PERIODNAME,
	V_STARTDATE,
	V_ENDDATE,
	V_CALENDARSEQ
FROM
	tcmp.cs_period
WHERE
	name = pperiodname
	AND removedate = c_eot
	AND calendarseq = V_CALENDARSEQ;


Execute Immediate 'truncate table ext.RPT_RUNPERIOD';
insert into ext.RPT_RUNPERIOD(
WITH period_types AS (
    SELECT 
        MAX(CASE WHEN name = 'month'      THEN periodtypeseq END) AS month_type,
        MAX(CASE WHEN LOWER(name) = 'quarter'    THEN periodtypeseq END) AS quarter_type,
        MAX(CASE WHEN LOWER(name) = 'semiannual' THEN periodtypeseq END) AS half_type,
        MAX(CASE WHEN LOWER(name) = 'year'       THEN periodtypeseq END) AS year_type
    FROM tcmp.cs_periodtype
    WHERE 1=1
      AND removedate = '2200-01-01'
),
target_period AS (
    -- Get the month user passed (e.g. October 2025)
    SELECT m.periodseq, m.startdate, m.enddate, m.parentseq
    FROM tcmp.cs_period m, period_types pt
    WHERE m.periodtypeseq = pt.month_type
      AND m.name = V_PERIODNAME --ut param
      AND m.removedate = '2200-01-01'
),
year_period AS (
    -- Find the year for that month
    SELECT y.periodseq, y.startdate, y.enddate
    FROM tcmp.cs_period m
    JOIN tcmp.cs_period q ON m.parentseq = q.periodseq
    JOIN tcmp.cs_period s ON q.parentseq = s.periodseq
    JOIN tcmp.cs_period y ON s.parentseq = y.periodseq
    , period_types pt, target_period tp
    WHERE m.periodtypeseq = pt.month_type
      AND q.periodtypeseq = pt.quarter_type
      AND s.periodtypeseq = pt.half_type
      AND y.periodtypeseq = pt.year_type
      AND m.periodseq = tp.periodseq
      AND q.removedate = '2200-01-01'
      AND s.removedate = '2200-01-01'
      AND y.removedate = '2200-01-01'
)
SELECT 
    m.periodseq      AS month_periodseq,
    m.name           AS month_name,
    m.startdate      AS month_startdate,
    m.enddate        AS month_enddate,

    q.periodseq      AS quarter_periodseq,
    q.name           AS quarter_name,
    q.startdate      AS quarter_startdate,
    q.enddate        AS quarter_enddate,

    s.periodseq      AS half_periodseq,
    s.name           AS half_name,
    s.startdate      AS half_startdate,
    s.enddate        AS half_enddate,

    y.periodseq      AS year_periodseq,
    y.name           AS year_name,
    y.startdate      AS year_startdate,
    y.enddate        AS year_enddate,
    m.calendarseq,
    RANK() OVER (ORDER BY m.periodseq,m.startdate) AS period_rank
FROM tcmp.cs_period m
JOIN tcmp.cs_period q ON m.parentseq = q.periodseq
JOIN tcmp.cs_period s ON q.parentseq = s.periodseq
JOIN tcmp.cs_period y ON s.parentseq = y.periodseq
, period_types pt, target_period tp, year_period yp
WHERE m.periodtypeseq = pt.month_type
  AND q.periodtypeseq = pt.quarter_type
  AND s.periodtypeseq = pt.half_type
  AND y.periodtypeseq = pt.year_type
  AND m.removedate = '2200-01-01'
  AND q.removedate = '2200-01-01'
  AND s.removedate = '2200-01-01'
  AND y.removedate = '2200-01-01'
  and m.calendarseq=V_CALENDARSEQ
  AND m.startdate BETWEEN yp.startdate AND add_days(tp.enddate,-1)
ORDER BY m.startdate);

commit;

EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.RPT_YTD_ATTIANMENT_PAYEE_DATA ';
INSERT INTO EXT.RPT_YTD_ATTIANMENT_PAYEE_DATA  (
with payeedata as (
SELECT     distinct     V_PUSEQ PROCESSINGUNITSEQ,
                 pd.periodseq periodseq,
                 pd.name periodname,
                 V_STARTDATE as STARTDATE, 
                 V_ENDDATE as ENDDATE,
                 pt.lastname || ' ' ||pt.firstname  ParticipantFullName,
                 pt.userid userid,
                 pe.payeeid ParticipantID,
                 pt.GenericAttribute5 Division,
                 pt.GenericAttribute7 Site,
                 pt.GenericAttribute4 Department,
                 pn.GenericAttribute5 Region,
                 pt.GenericAttribute3 LocalCurrency,
                  pn.TargetCompensation TargetCompensation,
                  pn.ruleelementownerseq as positionseq,pe.payeeseq
                  --DENSE_RANK() OVER (PARTITION BY pn.ruleelementownerseq, pd.periodseq ORDER BY pe.payeeseq) AS period_rank
            FROM tcmp.cs_participant pt,
                 tcmp.cs_payee pe,
                 tcmp.cs_businessunit bu,
                 tcmp.cs_position pn,
                 tcmp.cs_period pd,
                 tcmp.cs_calendar ca
           WHERE  pt.payeeseq = pe.payeeseq
           		AND ( pt.TERMINATIONDATE >= v_startdate OR pt.TERMINATIONDATE  IS NULL)
                 AND pe.businessunitmap = bu.MASK
                 AND pt.payeeseq = pn.payeeseq
                 AND pd.periodseq = V_PERIODSEQ
                 AND pd.removedate = c_eot
                 AND pd.calendarseq = ca.calendarseq
                 AND ca.removedate = c_eot
                 AND ca.NAME = V_CALENDARNAME
                 AND pt.removedate = c_eot
                 AND pe.removedate = c_eot
                 and pn.removedate=c_eot
                 AND ( (pt.effectivestartdate < pd.enddate
                        AND pt.effectiveenddate >= pd.enddate))
                 AND ( (pe.effectivestartdate < pd.enddate
                        AND pe.effectiveenddate >= pd.enddate))
                 AND pn.effectivestartdate < pd.enddate
                 AND pn.effectiveenddate >= pd.enddate)
                      select 	PROCESSINGUNITSEQ,
	PERIODSEQ,
	PERIODNAME,
	STARTDATE,
	ENDDATE,
	PARTICIPANTFULLNAME,
	USERID,
	PARTICIPANTID,
	DIVISION,
	SITE,
	DEPARTMENT,
	REGION,
	LOCALCURRENCY,
	TARGETCOMPENSATION,
	POSITIONSEQ,
	PAYEESEQ from payeedata
--	 where period_rank=1
);
 EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.RPT_YTD_ATTAINMENT_INCENTIVE ';
 INSERT INTO   EXT.RPT_YTD_ATTAINMENT_INCENTIVE (
with incentive as(
	select inc.positionseq,inc.payeeseq,inc.periodseq ,inc.genericattribute1,inc.genericattribute3,inc.genericattribute4,inc.genericattribute16,inc.value, inc.genericnumber1,inc.genericnumber2,inc.genericnumber3,inc.genericnumber4,inc.genericnumber5,inc.genericnumber6 ,p.month_name,p.quarter_name,p.period_rank 
	from cs_incentive inc inner join EXT.RPT_RUNPERIOD p
	on inc.periodseq=p.month_periodseq
	where coalesce(inc.genericboolean1,0)=1
	and inc.processingunitseq=V_PUSEQ
	and inc.genericattribute16  in ('YTD Target Attainment' , 'Monthly Target Attainment','Guarantee')
	union all
	select inc.positionseq,inc.payeeseq,inc.periodseq ,inc.genericattribute1,inc.genericattribute3,inc.genericattribute4,inc.genericattribute16,inc.value, inc.genericboolean3 as genericnumber1,inc.genericnumber2,inc.genericnumber3,inc.genericnumber4,inc.genericnumber5,inc.genericnumber6 ,p.month_name,p.quarter_name,p.period_rank 
	from cs_incentive inc inner join EXT.RPT_RUNPERIOD p
	on inc.periodseq=p.month_periodseq
	where coalesce(inc.genericboolean3,0)=1
	and inc.processingunitseq=V_PUSEQ
--	and coalesce(inc.genericboolean1,0)=1
)select * from incentive order by positionseq,period_rank);
commit;

 EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.RPT_YTD_ATTAINMENT_INCENTIVE_EXTRACT ';
INSERT INTO EXT.RPT_YTD_ATTAINMENT_INCENTIVE_EXTRACT (
SELECT 
   "Month",
   "Qtr",
   "ID",
   "Plan Participant",
   Division,
   Site,
   Department,
   Region,
   "Currency" as "Currency",
   max("Plan Elements") AS "Plan Elements",
   max("Quota Plan") AS "Quota Plan",
   max("Quota") AS "Quota",
   max("TI LOC (Annualized)") AS "TI LOC (Annualized)",
   max("TI USD (Annualized)") AS "TI USD (Annualized)",
   SUM("YTD Attainment$ (USD)") AS "YTD Attainment$ (USD)",
   SUM("YTD Attainment%") AS "YTD Attainment%",
   max("YTD Comm Earned%") AS "YTD Comm Earned%",
   SUM("YTD Commission Earned (LOC)") AS "YTD Commission Earned (LOC)",
   SUM("YTD Commission Earned (USD)") AS "YTD Commission Earned (USD)",
   SUM("Current Month Revenue Attainment") AS "Current Month Revenue Attainment",
   SUM("Current Month Commission Earned (LOC)") AS "Current Month Commission Earned (LOC)",
   SUM("Current Month Commission Earned (USD)") AS "Current Month Commission Earned (USD)",
   SUM("Guarantee Payout Amount") AS "Guarantee Payout Amount"
FROM (
    -- first select
    SELECT 
        inc.month_name        AS "Month",
        inc.quarter_name      AS "Qtr",
        pay.PARTICIPANTID     AS "ID",
        pay.PARTICIPANTFULLNAME AS "Plan Participant",
        pay.Division,
        pay.Site,
        pay.Department,
        pay.Region,
        pay.LocalCurrency     AS "Currency",
        inc.GENERICATTRIBUTE1 AS "Plan Elements",
        inc.GENERICATTRIBUTE3 AS "Quota Plan",
        inc.GENERICNUMBER1    AS "Quota",
        case when inc.genericnumber6 = 0 then pay.TargetCompensation
        else inc.genericnumber6 end AS "TI LOC (Annualized)",
        ( case when CAST(inc.genericnumber6 AS DECIMAL(18,2)) = 0 then pay.TargetCompensation
         else CAST(inc.genericnumber6 AS DECIMAL(18,2)) end)  *
           (case  when TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) <> 0 then 
           TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', ''))/100
           else 0 end )
           
           
           AS "TI USD (Annualized)",
        inc.GENERICNUMBER2    AS "YTD Attainment$ (USD)",
        inc.GENERICNUMBER3    AS "YTD Attainment%",
        NULL   AS "YTD Comm Earned%",
        NULL           AS "YTD Commission Earned (LOC)",
NULL AS "YTD Commission Earned (USD)",
        inc.GENERICNUMBER5    AS "Current Month Revenue Attainment",
        inc.value             AS "Current Month Commission Earned (LOC)",
        inc.value *
 (case  when TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) <> 0 then 
           TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', ''))/100
           else 0 end )
           AS "Current Month Commission Earned (USD)",
           NUll as "Guarantee Payout Amount",
           CASE 
          WHEN inc.month_name LIKE 'July%' THEN 1
          WHEN inc.month_name LIKE 'August%' THEN 2
          WHEN inc.month_name LIKE 'September%' THEN 3
          WHEN inc.month_name LIKE 'October%' THEN 4
          WHEN inc.month_name LIKE 'November%' THEN 5
          WHEN inc.month_name LIKE 'December%' THEN 6
          WHEN inc.month_name LIKE 'January%' THEN 7
          WHEN inc.month_name LIKE 'February%' THEN 8
          WHEN inc.month_name LIKE 'March%' THEN 9
          WHEN inc.month_name LIKE 'April%' THEN 10
          WHEN inc.month_name LIKE 'May%' THEN 11
          WHEN inc.month_name LIKE 'June%' THEN 12
        END AS month_order
    FROM EXT.RPT_YTD_ATTAINMENT_INCENTIVE inc
    JOIN EXT.RPT_YTD_ATTIANMENT_PAYEE_DATA pay
      ON inc.POSITIONSEQ = pay.POSITIONSEQ
     AND inc.PAYEESEQ = pay.PAYEESEQ
    WHERE inc.GENERICATTRIBUTE16 = 'Monthly Target Attainment'

    UNION ALL

    -- second select
    SELECT 
        inc.month_name        AS "Month",
        inc.quarter_name      AS "Qtr",
        pay.PARTICIPANTID     AS "ID",
        pay.PARTICIPANTFULLNAME AS "Plan Participant",
        pay.Division,
        pay.Site,
        pay.Department,
        pay.Region,
        pay.LocalCurrency     AS "Currency",
        NULL AS "Plan Elements",
        NULL AS "Quota Plan",
        NULL AS "Quota",
        NULL AS "TI LOC (Annualized)",
        NULL AS "TI USD (Annualized)",
        NULL AS "YTD Attainment$ (USD)",
        NULL AS "YTD Attainment%",
        NULL   AS "YTD Comm Earned%",
        inc.value             AS "YTD Commission Earned (LOC)",
        inc.value *
            (case  when TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) <> 0 then 
           TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', ''))/100
           else 0 end )
           AS "YTD Commission Earned (USD)",
        NULL AS "Current Month Revenue Attainment",
        NULL AS "Current Month Commission Earned (LOC)",
        NULL AS "Current Month Commission Earned (USD)",
        NULL as "Guarantee Payout Amount",
        CASE 
          WHEN inc.month_name LIKE 'July%' THEN 1
          WHEN inc.month_name LIKE 'August%' THEN 2
          WHEN inc.month_name LIKE 'September%' THEN 3
          WHEN inc.month_name LIKE 'October%' THEN 4
          WHEN inc.month_name LIKE 'November%' THEN 5
          WHEN inc.month_name LIKE 'December%' THEN 6
          WHEN inc.month_name LIKE 'January%' THEN 7
          WHEN inc.month_name LIKE 'February%' THEN 8
          WHEN inc.month_name LIKE 'March%' THEN 9
          WHEN inc.month_name LIKE 'April%' THEN 10
          WHEN inc.month_name LIKE 'May%' THEN 11
          WHEN inc.month_name LIKE 'June%' THEN 12
        END AS month_order
    FROM EXT.RPT_YTD_ATTAINMENT_INCENTIVE inc
    JOIN EXT.RPT_YTD_ATTIANMENT_PAYEE_DATA pay
      ON inc.POSITIONSEQ = pay.POSITIONSEQ
     AND inc.PAYEESEQ = pay.PAYEESEQ
    WHERE inc.GENERICATTRIBUTE16 = 'YTD Target Attainment'
    
    
      UNION ALL

    -- second select
    SELECT 
        inc.month_name        AS "Month",
        inc.quarter_name      AS "Qtr",
        pay.PARTICIPANTID     AS "ID",
        pay.PARTICIPANTFULLNAME AS "Plan Participant",
        pay.Division,
        pay.Site,
        pay.Department,
        pay.Region,
        pay.LocalCurrency     AS "Currency",
        NULL AS "Plan Elements",
        NULL AS "Quota Plan",
        NULL AS "Quota",
        case when inc.genericnumber6 = 0 then pay.TargetCompensation
        else inc.genericnumber6 end  AS "TI LOC (Annualized)",
          ( case when CAST(inc.genericnumber6 AS DECIMAL(18,2)) = 0 then pay.TargetCompensation
         else CAST(inc.genericnumber6 AS DECIMAL(18,2)) end)  *
           (case  when TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) <> 0 then 
           TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(inc.genericattribute4, ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', ''))/100
           else 0 end )
           AS "TI USD (Annualized)",
        NULL AS "YTD Attainment$ (USD)",
        NULL AS "YTD Attainment%",
         NULL  AS "YTD Comm Earned%",
        null   AS "YTD Commission Earned (LOC)",
        null AS "YTD Commission Earned (USD)",
        NULL AS "Current Month Revenue Attainment",
        NULL AS "Current Month Commission Earned (LOC)",
        NULL AS "Current Month Commission Earned (USD)",
        inc.value as "Guarantee Payout Amount",
        CASE 
          WHEN inc.month_name LIKE 'July%' THEN 1
          WHEN inc.month_name LIKE 'August%' THEN 2
          WHEN inc.month_name LIKE 'September%' THEN 3
          WHEN inc.month_name LIKE 'October%' THEN 4
          WHEN inc.month_name LIKE 'November%' THEN 5
          WHEN inc.month_name LIKE 'December%' THEN 6
          WHEN inc.month_name LIKE 'January%' THEN 7
          WHEN inc.month_name LIKE 'February%' THEN 8
          WHEN inc.month_name LIKE 'March%' THEN 9
          WHEN inc.month_name LIKE 'April%' THEN 10
          WHEN inc.month_name LIKE 'May%' THEN 11
          WHEN inc.month_name LIKE 'June%' THEN 12
        END AS month_order
    FROM EXT.RPT_YTD_ATTAINMENT_INCENTIVE inc
    JOIN EXT.RPT_YTD_ATTIANMENT_PAYEE_DATA pay
      ON inc.POSITIONSEQ = pay.POSITIONSEQ
     AND inc.PAYEESEQ = pay.PAYEESEQ
    WHERE inc.GENERICATTRIBUTE16 = 'Guarantee'
    
    UNION ALL
     -- second select
    SELECT 
        inc.month_name        AS "Month",
        inc.quarter_name      AS "Qtr",
        pay.PARTICIPANTID     AS "ID",
        pay.PARTICIPANTFULLNAME AS "Plan Participant",
        pay.Division,
        pay.Site,
        pay.Department,
        pay.Region,
        pay.LocalCurrency     AS "Currency",
        NULL AS "Plan Elements",
        NULL AS "Quota Plan",
        NULL AS "Quota",
        NULL AS "TI LOC (Annualized)",
        NULL AS "TI USD (Annualized)",
        NULL AS "YTD Attainment$ (USD)",
        NULL AS "YTD Attainment%",
        inc.genericnumber3   AS "YTD Comm Earned%",
        null        AS "YTD Commission Earned (LOC)",
        null
           AS "YTD Commission Earned (USD)",
        NULL AS "Current Month Revenue Attainment",
        NULL AS "Current Month Commission Earned (LOC)",
        NULL AS "Current Month Commission Earned (USD)",
        NULL as "Guarantee Payout Amount",
        CASE 
          WHEN inc.month_name LIKE 'July%' THEN 1
          WHEN inc.month_name LIKE 'August%' THEN 2
          WHEN inc.month_name LIKE 'September%' THEN 3
          WHEN inc.month_name LIKE 'October%' THEN 4
          WHEN inc.month_name LIKE 'November%' THEN 5
          WHEN inc.month_name LIKE 'December%' THEN 6
          WHEN inc.month_name LIKE 'January%' THEN 7
          WHEN inc.month_name LIKE 'February%' THEN 8
          WHEN inc.month_name LIKE 'March%' THEN 9
          WHEN inc.month_name LIKE 'April%' THEN 10
          WHEN inc.month_name LIKE 'May%' THEN 11
          WHEN inc.month_name LIKE 'June%' THEN 12
        END AS month_order
    FROM EXT.RPT_YTD_ATTAINMENT_INCENTIVE inc
    JOIN EXT.RPT_YTD_ATTIANMENT_PAYEE_DATA pay
      ON inc.POSITIONSEQ = pay.POSITIONSEQ
     AND inc.PAYEESEQ = pay.PAYEESEQ
    WHERE inc.genericnumber1=1
) sub
GROUP BY 
   "Month","Qtr","ID","Plan Participant",
   Division,Site,Department,Region,"Currency",month_order
ORDER BY "Plan Participant",month_order
);


header =
SELECT
    'Month'                                AS "Month",
    'Qtr'                                  AS "Qtr",
    'ID'                                   AS "ID",
    'Plan Participant'                     AS "Plan Participant",
    'DIVISION'                             AS "DIVISION",
    'SITE'                                 AS "SITE",
    'DEPARTMENT'                           AS "DEPARTMENT",
    'REGION'                               AS "REGION",
    'Currency'                             AS "Currency",
    'Plan Elements'                        AS "Plan Elements",
    'Quota Plan'                           AS "Quota Plan",
    'Quota'                                AS "Quota",
    'TI LOC (Annualized)'                  AS "TI LOC (Annualized)",
    'TI USD (Annualized)'                  AS "TI USD (Annualized)",
    'YTD Attainment$ (USD)'                AS "YTD Attainment$ (USD)",
    'YTD Attainment%'                      AS "YTD Attainment%",
    'YTD Comm Earned%'                      AS "YTD Comm Earned%",
    'YTD Commission Earned (LOC)'          AS "YTD Commission Earned (LOC)",
    'YTD Commission Earned (USD)'          AS "YTD Commission Earned (USD)",
    'Current Month Revenue Attainment'     AS "Current Month Revenue Attainment",
    'Current Month Commission Earned (LOC)' AS "Current Month Commission Earned (LOC)",
    'Current Month Commission Earned (USD)' AS "Current Month Commission Earned (USD)",
    'Guarantee Payout Amount' AS "Guarantee Payout Amount"
FROM dummy;

details=
SELECT 
    "Month",
    "Qtr",
    "ID",
    "Plan Participant",
    "DIVISION",
    "SITE",
    "DEPARTMENT",
    "REGION",
    "Currency",
    "Plan Elements",
    "Quota Plan",
    '$' || CAST(TO_DECIMAL("Quota", 20, 4) AS NVARCHAR(255)) AS "Quota",
    CAST(TO_DECIMAL("TI LOC (Annualized)", 20, 4) AS NVARCHAR(255)) AS "TI LOC (Annualized)",
    CAST(TO_DECIMAL("TI USD (Annualized)", 20, 4) AS NVARCHAR(255)) AS "TI USD (Annualized)",
    '$' || CAST(TO_DECIMAL("YTD Attainment$ (USD)", 20, 4) AS NVARCHAR(255)) AS "YTD Attainment$ (USD)",
    CAST(ROUND("YTD Attainment%" * 100, 4) AS NVARCHAR(255)) || '%' AS "YTD Attainment%",
    CAST(ROUND("YTD Comm Earned%" * 100, 4) AS NVARCHAR(255)) || '%' AS "YTD Comm Earned%",
    CAST(TO_DECIMAL("YTD Commission Earned (LOC)", 20, 4) AS NVARCHAR(255)) AS "YTD Commission Earned (LOC)",
    '$' || CAST(TO_DECIMAL("YTD Commission Earned (USD)", 20, 4) AS NVARCHAR(255)) AS "YTD Commission Earned (USD)",
    CAST(TO_DECIMAL("Current Month Revenue Attainment", 20, 4) AS NVARCHAR(255)) AS "Current Month Revenue Attainment",
    CAST(TO_DECIMAL("Current Month Commission Earned (LOC)", 20, 4) AS NVARCHAR(255)) AS "Current Month Commission Earned (LOC)",
    '$' || CAST(TO_DECIMAL("Current Month Commission Earned (USD)", 20, 4) AS NVARCHAR(255)) AS "Current Month Commission Earned (USD)",
    CAST(TO_DECIMAL("Guarantee Payout Amount", 20, 4) AS NVARCHAR(255)) AS "Guarantee Payout Amount"
FROM "EXT"."RPT_YTD_ATTAINMENT_INCENTIVE_EXTRACT";



EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.RPT_YTD_ATTAINMENT_INCENTIVE_OUT';
insert into EXT.RPT_YTD_ATTAINMENT_INCENTIVE_OUT(
select * from :header
union
select * from :details);
commit;


V_PERIODNAME_NOSPACE := REPLACE(V_PERIODNAME, ' ', '_');

FILENAME := 'YTD_ATTAINMENT_DETAIL_' || V_PERIODNAME_NOSPACE || '_' || to_Varchar(CURRENT_DATE,
'YYYYMMDD')|| '.csv';

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ATTAINMENT _DETAIL Procedure Completed ' || V_PERIODNAME_NOSPACE,
	5
FROM
	dummy ;

COMMIT;
END