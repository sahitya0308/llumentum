CREATE OR REPLACE  PROCEDURE EXT.SP_RPT_ACCRUAL_DETAIL_OLD(OUT FILENAME VARCHAR(100),
pprocessingunitname VARCHAR(100),
pperiodname VARCHAR(100)) LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS V_PERIODNAME varchar2(255);

V_PERIODNAME_NOSPACE varchar2(255);
V_CALENDARNAME varchar2(255) := 'LITE Fiscal Calendar';
V_PERIODSEQ number;
V_CALENDARSEQ number;
V_PUSEQ number ;
V_STARTDATE DATE;
V_ENDDATE DATE;
c_eot DATE := to_date('01012200','MMDDYYYY') ;

BEGIN
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL Exception Occurred - ' ||::SQL_ERROR_CODE || ' - ' ||::SQL_ERROR_MESSAGE ,
	99
FROM
	dummy ;

INSERT 	INTO 	EXT.RPT_DEBUG
SELECT 	'0046', 	NOW(),	'SP_RPT_ACCRUAL_DETAIL procedure started', 	1 FROM 	dummy ;

CALL "EXT"."SP_RPT_ORGDATA" (pprocessingunitname ,pperiodname);

SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL ORGDATA Proc Completed',
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

DELETE
FROM
	EXT.RPT_ACCRUALDETAIL_TMP;

DELETE
FROM
	EXT.RPT_ACCRUALDETAIL;

COMMIT;

INSERT
	INTO
	EXT.RPT_ACCRUALDETAIL_TMP
SELECT
	V_PERIODSEQ,
	v_periodname,
	V_PUSEQ,
	pprocessingunitname,
	h.payeeseq,
	h.positionseq,
	h.payeeid,
	h.fullname,
	h.mfullname_L1,
	h.mfullname_L2,
	h.division,
	SUBSTRING(h.cc_dept, 1, 4) "Dept Code",
	c.name,
	ct.credittypeid,
	CASE WHEN ct.credittypeid LIKE 'Invoice Revenue%'
	AND c.name LIKE 'CO%' THEN 'Direct Receiver'
	WHEN ct.credittypeid ='Invoice Revenue'
	AND c.name LIKE 'ICO%' THEN 'Roll up'
	ELSE ct.credittypeid
END "Credit Type",
st.salestransactionseq,
c.creditseq,
co.commissionseq,
sinc.incentiveseq,
tinc.incentiveseq,
st.sublinenumber "Subline",
st.accountingdate,
so.orderid "Order ID",
st.genericdate2 "Order Line Shipped Date",
st.compensationdate "Compensation Date",
et.eventtypeid,
CASE WHEN et.eventtypeid IN ('Invoice',
'Credit Memo',
'Debit Memo') THEN 'Lumentum Invoice'
ELSE et.eventtypeid
END "Event Type",
IFNULL(st.nativecurrencyamount, 0) "Native Amount",
st.nativecurrency "Native Currency",
TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sinc.genericattribute4 , ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) "Exchange Rate",
(co.value / co.ratevalue) "Transaction Amount (USD)",
--st.VALUE "Transaction Amount (USD)",
sinc.genericnumber1 "Quota",
sinc.genericnumber6 "TI (LOC)",
case when ifnull(sinc.genericnumber6,0)=0 then 0
else ROUND(((co.ratevalue * sinc.genericnumber1)/ sinc.genericnumber6)) END"Tier rate",
co.ratevalue,
co.value "Commission (LC)",
h.local_currency "Commission Local Currency",
(co.value * TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sinc.genericattribute4 , ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', ''))/ 100) "Commission (USD)",
NULL,
--bta.country,
 st.genericattribute2 "Bill to County",
NULL,
--bta.state,
 st.genericattribute4 "L8 Item Number",
st.genericattribute8 "L2 Product",
st.genericattribute9 "L3 Product",
st.genericattribute10 "L4 Product",
st.genericattribute11 "L5 Product",
st.genericattribute12 "L6 Product",
st.productid "Product ID",
st.productdescription "Product Description",
NULL "Customer ID",
--bta.custid 
 NULL "Company",
--bta.company 
 st.genericattribute6 "Invoice Line Number",
st.numberofunits "Number of Units",
st.genericattribute21 "Revenue Account",
sinc.genericattribute1 "Plan Element",
st.comments,
st.genericattribute20 "Customer PO Number",
st.genericdate3 "Order Line Booked Date"
FROM
EXT.RPT_ORGDATA h,
TCMP.cs_incentive sinc,
TCMP.cs_incentiveselftrace ist,
TCMP.cs_incentive tinc,
tcmp.cs_credit c,
cs_credittype ct,
tcmp.cs_commission co,
tcmp.cs_salestransaction st,
tcmp.cs_eventtype et,
tcmp.cs_salesorder so
WHERE
h.processingunitseq = V_PUSEQ
AND h.periodseq = V_PERIODSEQ
AND IFNULL(sinc.genericboolean1, 0) = 1
AND TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sinc.genericattribute2 , ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) > 0
AND h.periodseq = sinc.periodseq
AND h.positionseq = sinc.positionseq
AND ist.sourceperiodseq = sinc.periodseq
AND sinc.incentiveseq = ist.targetincentiveseq
AND ist.sourceincentiveseq = tinc.incentiveseq
AND tinc.periodseq = co.periodseq
AND tinc.processingunitseq = co.processingunitseq
AND tinc.positionseq = co.positionseq
AND tinc.incentiveseq = co.incentiveseq
AND c.periodseq = co.periodseq
AND c.processingunitseq = co.processingunitseq
AND c.positionseq = co.positionseq
AND c.creditseq = co.creditseq
AND ct.removedate = c_eot
AND ct.datatypeseq = c.credittypeseq
AND c.salestransactionseq = st.salestransactionseq
AND c.processingunitseq = st.processingunitseq
AND st.modelseq = 0
AND so.removedate = c_eot
AND so.salesorderseq = st.salesorderseq
AND et.removedate = c_eot
AND et.datatypeseq = st.eventtypeseq;

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL procedure - Inserted Per Credit Commission records into RPT_ACCRUALDETAIL_TMP table',
	2
FROM
	dummy ;

COMMIT;
--Adding MBO records
 INSERT
	INTO
	EXT.RPT_ACCRUALDETAIL_TMP
SELECT
	V_PERIODSEQ,
	v_periodname,
	V_PUSEQ,
	pprocessingunitname,
	h.payeeseq,
	h.positionseq,
	h.payeeid,
	h.fullname,
	h.mfullname_L1,
	h.mfullname_L2,
	h.division,
	SUBSTRING(h.cc_dept, 1, 4) "Dept Code",
	c.name,
	ct.credittypeid,
	CASE WHEN ct.credittypeid LIKE 'Invoice Revenue%'
	AND c.name LIKE 'CO%' THEN 'Direct Receiver'
	WHEN ct.credittypeid ='Invoice Revenue'
	AND c.name LIKE 'ICO%' THEN 'Roll up'
	ELSE ct.credittypeid
END "Credit Type",
st.salestransactionseq,
c.creditseq,
NULL,
sinc.incentiveseq,
tinc.incentiveseq,
st.sublinenumber "Subline",
st.accountingdate,
so.orderid "Order ID",
st.genericdate2 "Order Line Shipped Date",
st.compensationdate "Compensation Date",
et.eventtypeid,
CASE WHEN et.eventtypeid IN ('Invoice',
'Credit Memo',
'Debit Memo') THEN 'Lumentum Invoice'
ELSE et.eventtypeid
END "Event Type",
(c.genericnumber2*100) "Native Amount",
st.nativecurrency "Native Currency",
(c.genericnumber3*100) "Exchange Rate",
(c.genericnumber2*100) "Transaction Amount (USD)",
--st.value "Transaction Amount (USD)",
NULL "Quota",
c.genericnumber6 "TI (LOC)",
NULL "Tier rate",
NULL,
c.value "Commission (LC)",
h.local_currency "Commission Local Currency",
(c.value * c.genericnumber3) "Commission (USD)",
NULL,
--bta.country,
 st.genericattribute2 "Bill to County",
NULL,
--bta.state,
 st.genericattribute4 "L8 Item Number",
st.genericattribute8 "L2 Product",
st.genericattribute9 "L3 Product",
st.genericattribute10 "L4 Product",
st.genericattribute11 "L5 Product",
st.genericattribute12 "L6 Product",
st.productid "Product ID",
st.productdescription "Product Description",
NULL "Customer ID",
--bta.custid 
 NULL "Company",
--bta.company 
 st.genericattribute6 "Invoice Line Number",
st.numberofunits "Number of Units",
st.genericattribute21 "Revenue Account",
sinc.genericattribute1 "Plan Element",
st.comments,
st.genericattribute20 "Customer PO Number",
st.genericdate3 "Order Line Booked Date"
--select pmc.*

FROM EXT.RPT_ORGDATA h,
TCMP.cs_incentive sinc,
TCMP.cs_incentiveselftrace ist,
TCMP.cs_incentive tinc,
CS_INCENTIVEPMTRACE ipm,
cs_measurement m,
CS_PMCREDITTRACE pmc,
tcmp.cs_credit c,
cs_credittype ct,
tcmp.cs_salestransaction st,
tcmp.cs_eventtype et,
tcmp.cs_salesorder so
WHERE
h.processingunitseq = V_PUSEQ
AND h.periodseq = V_PERIODSEQ
--and h.positionname = '68414'
AND IFNULL(sinc.genericboolean1, 0) = 1
AND sinc.genericattribute1 = 'MBO'
AND TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sinc.genericattribute2 , ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) > 0
AND h.periodseq = sinc.periodseq
AND h.positionseq = sinc.positionseq
AND ist.sourceperiodseq = sinc.periodseq
AND sinc.incentiveseq = ist.targetincentiveseq
AND ist.sourceincentiveseq = tinc.incentiveseq
AND tinc.incentiveseq = ipm.incentiveseq
AND tinc.periodseq = ipm.targetperiodseq
AND tinc.processingunitseq = ipm.processingunitseq
AND m.measurementseq = ipm.measurementseq
AND m.periodseq = ipm.targetperiodseq
AND m.processingunitseq = ipm.processingunitseq
AND m.measurementseq = pmc.measurementseq
AND m.periodseq = pmc.targetperiodseq
AND m.processingunitseq = pmc.processingunitseq
AND c.periodseq = pmc.targetperiodseq
AND c.processingunitseq = pmc.processingunitseq
AND c.creditseq = pmc.creditseq
AND ct.removedate = c_eot
AND ct.datatypeseq = c.credittypeseq
AND c.salestransactionseq = st.salestransactionseq
AND c.processingunitseq = st.processingunitseq
AND st.modelseq = 0
AND so.removedate = c_eot
AND so.salesorderseq = st.salesorderseq
AND et.removedate = c_eot
AND et.datatypeseq = st.eventtypeseq;

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL procedure - Inserted MBO records into RPT_ACCRUALDETAIL_TMP table',
	2.1
FROM
	dummy ;

COMMIT;

--Adding YOY Growth records
 INSERT
	INTO
	EXT.RPT_ACCRUALDETAIL_TMP
SELECT
	V_PERIODSEQ,
	v_periodname,
	V_PUSEQ,
	pprocessingunitname,
	h.payeeseq,
	h.positionseq,
	h.payeeid,
	h.fullname,
	h.mfullname_L1,
	h.mfullname_L2,
	h.division,
	SUBSTRING(h.cc_dept, 1, 4) "Dept Code",
	c.name,
	ct.credittypeid,
	CASE WHEN ct.credittypeid LIKE 'Invoice Revenue%'
	AND c.name LIKE 'CO%' THEN 'Direct Receiver'
	WHEN ct.credittypeid = 'Invoice Revenue'
	AND c.name LIKE 'ICO%' THEN 'Roll up'
	ELSE ct.credittypeid
END "Credit Type",
st.salestransactionseq,
c.creditseq,
NULL,
sinc.incentiveseq,
NULL,
st.sublinenumber "Subline",
st.accountingdate,
so.orderid "Order ID",
st.genericdate2 "Order Line Shipped Date",
st.compensationdate "Compensation Date",
et.eventtypeid,
CASE WHEN et.eventtypeid IN ('Invoice',
'Credit Memo',
'Debit Memo') THEN 'Lumentum Invoice'
ELSE et.eventtypeid
END "Event Type",
(c.value*100) "Native Amount",
st.nativecurrency "Native Currency",
--(sinc.genericattribute4) "Exchange Rate",
TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Sinc.genericattribute4 , ',',''), '%',''),'USD',''), 'quantity',''),'integer','')) "Exchange Rate",
(c.value*100) "Transaction Amount (USD)",
--st.value "Transaction Amount (USD)",
NULL "Quota",
c.genericnumber6 "TI (LOC)",
NULL "Tier rate",
NULL,
sinc.value "Commission (LC)",
h.local_currency "Commission Local Currency",
(sinc.value * TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Sinc.genericattribute4 , ',',''), '%',''),'USD',''), 'quantity',''),'integer',''))/100)  "Commission (USD)",
NULL,
--bta.country,
 st.genericattribute2 "Bill to County",
NULL,
--bta.state,
 st.genericattribute4 "L8 Item Number",
st.genericattribute8 "L2 Product",
st.genericattribute9 "L3 Product",
st.genericattribute10 "L4 Product",
st.genericattribute11 "L5 Product",
st.genericattribute12 "L6 Product",
st.productid "Product ID",
st.productdescription "Product Description",
NULL "Customer ID",
--bta.custid 
 NULL "Company",
--bta.company 
 st.genericattribute6 "Invoice Line Number",
st.numberofunits "Number of Units",
st.genericattribute21 "Revenue Account",
sinc.genericattribute1 "Plan Element",
st.comments,
st.genericattribute20 "Customer PO Number",
st.genericdate3 "Order Line Booked Date"
--select pmc.*

FROM EXT.RPT_ORGDATA h,
TCMP.cs_incentive sinc,
CS_INCENTIVEPMTRACE ipm,
cs_measurement m,
CS_PMCREDITTRACE pmc,
tcmp.cs_credit c,
cs_credittype ct,
tcmp.cs_salestransaction st,
tcmp.cs_eventtype et,
tcmp.cs_salesorder so
WHERE
h.processingunitseq = V_PUSEQ
AND h.periodseq = V_PERIODSEQ
--and h.positionname = '68414'
AND IFNULL(sinc.genericboolean1, 0) = 1
AND sinc.genericattribute1 = 'YOY Growth'
AND TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sinc.genericattribute2 , ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) > 0
AND h.periodseq = sinc.periodseq
AND h.positionseq = sinc.positionseq
AND sinc.incentiveseq = ipm.incentiveseq
AND sinc.periodseq = ipm.targetperiodseq
AND sinc.processingunitseq = ipm.processingunitseq
AND m.measurementseq = ipm.measurementseq
AND m.periodseq = ipm.targetperiodseq
AND m.processingunitseq = ipm.processingunitseq
AND m.measurementseq = pmc.measurementseq
AND m.periodseq = pmc.targetperiodseq
AND m.processingunitseq = pmc.processingunitseq
AND c.periodseq = pmc.targetperiodseq
AND c.processingunitseq = pmc.processingunitseq
AND c.creditseq = pmc.creditseq
AND ct.removedate = c_eot
AND ct.datatypeseq = c.credittypeseq
AND c.salestransactionseq = st.salestransactionseq
AND c.processingunitseq = st.processingunitseq
AND st.modelseq = 0
AND so.removedate = c_eot
AND so.salesorderseq = st.salesorderseq
AND et.removedate = c_eot
AND et.datatypeseq = st.eventtypeseq;

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL procedure - Inserted YOY Growth records into RPT_ACCRUALDETAIL_TMP table',
	2.1
FROM
	dummy ;
	
--Adding CIP and Payment Adjustment records
 INSERT
	INTO
	EXT.RPT_ACCRUALDETAIL_TMP
SELECT
	V_PERIODSEQ,
	v_periodname,
	V_PUSEQ,
	pprocessingunitname,
	h.payeeseq,
	h.positionseq,
	h.payeeid,
	h.fullname,
	h.mfullname_L1,
	h.mfullname_L2,
	h.division,
	SUBSTRING(h.cc_dept, 1, 4) "Dept Code",
	c.name,
	ct.credittypeid,
	CASE WHEN ct.credittypeid LIKE 'Invoice Revenue%'
	AND c.name LIKE 'CO%' THEN 'Direct Receiver'
	WHEN ct.credittypeid ='Invoice Revenue'
	AND c.name LIKE 'ICO%' THEN 'Roll up'
	ELSE ct.credittypeid
END "Credit Type",
st.salestransactionseq,
c.creditseq,
NULL,
sinc.incentiveseq,
NULL,
st.sublinenumber "Subline",
st.accountingdate,
so.orderid "Order ID",
st.genericdate2 "Order Line Shipped Date",
st.compensationdate "Compensation Date",
et.eventtypeid,
CASE WHEN et.eventtypeid IN ('Invoice',
'Credit Memo',
'Debit Memo') THEN 'Lumentum Invoice'
ELSE et.eventtypeid
END "Event Type",
st.nativecurrencyamount "Native Amount",
st.nativecurrency "Native Currency",
(c.genericnumber3*100) "Exchange Rate",
(c.genericnumber2*100) "Transaction Amount (USD)",
--st.value "Transaction Amount (USD)",
NULL "Quota",
c.genericnumber6 "TI (LOC)",
NULL "Tier rate",
NULL,
c.value "Commission (LC)",
h.local_currency "Commission Local Currency",
(c.value * c.genericnumber3) "Commission (USD)",
NULL,
--bta.country,
 st.genericattribute2 "Bill to County",
NULL,
--bta.state,
 st.genericattribute4 "L8 Item Number",
st.genericattribute8 "L2 Product",
st.genericattribute9 "L3 Product",
st.genericattribute10 "L4 Product",
st.genericattribute11 "L5 Product",
st.genericattribute12 "L6 Product",
st.productid "Product ID",
st.productdescription "Product Description",
NULL "Customer ID",
--bta.custid 
 NULL "Company",
--bta.company 
 st.genericattribute6 "Invoice Line Number",
st.numberofunits "Number of Units",
st.genericattribute21 "Revenue Account",
sinc.genericattribute1 "Plan Element",
st.comments,
st.genericattribute20 "Customer PO Number",
st.genericdate3 "Order Line Booked Date"
FROM
EXT.RPT_ORGDATA h,
TCMP.cs_incentive sinc,
CS_INCENTIVEPMTRACE ipm,
cs_measurement m,
CS_PMCREDITTRACE pmc,
tcmp.cs_credit c,
cs_credittype ct,
tcmp.cs_salestransaction st,
tcmp.cs_eventtype et,
tcmp.cs_salesorder so
WHERE
h.processingunitseq = V_PUSEQ
AND h.periodseq = V_PERIODSEQ
AND IFNULL(sinc.genericboolean1, 0) = 1
AND sinc.genericattribute1 IN ('CIP',
'Payment Adjustment')
AND h.periodseq = sinc.periodseq
AND h.positionseq = sinc.positionseq
AND sinc.incentiveseq = ipm.incentiveseq
AND sinc.periodseq = ipm.targetperiodseq
AND sinc.processingunitseq = ipm.processingunitseq
AND m.measurementseq = ipm.measurementseq
AND m.periodseq = ipm.targetperiodseq
AND m.processingunitseq = ipm.processingunitseq
AND m.measurementseq = pmc.measurementseq
AND m.periodseq = pmc.targetperiodseq
AND m.processingunitseq = pmc.processingunitseq
AND c.periodseq = pmc.targetperiodseq
AND c.processingunitseq = pmc.processingunitseq
AND c.creditseq = pmc.creditseq
AND ct.removedate = c_eot
AND ct.datatypeseq = c.credittypeseq
AND c.salestransactionseq = st.salestransactionseq
AND c.processingunitseq = st.processingunitseq
AND st.modelseq = 0
AND so.removedate = c_eot
AND so.salesorderseq = st.salesorderseq
AND et.removedate = c_eot
AND et.datatypeseq = st.eventtypeseq;

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL procedure - Inserted CIP and Payment Adjustment records into RPT_ACCRUALDETAIL_TMP table',
	3.2
FROM
	dummy ;

COMMIT;
--Adding Guarantee records
 INSERT
	INTO
	EXT.RPT_ACCRUALDETAIL_TMP
SELECT
	V_PERIODSEQ,
	v_periodname,
	V_PUSEQ,
	pprocessingunitname,
	h.payeeseq,
	h.positionseq,
	h.payeeid,
	h.fullname,
	h.mfullname_L1,
	h.mfullname_L2,
	h.division,
	SUBSTRING(h.cc_dept, 1, 4) "Dept Code",
	'Guarantee' name,
	NULL credittypeid,
	sinc.GENERICATTRIBUTE1 "Credit Type",
	NULL salestransactionseq,
	NULL creditseq,
	NULL,
	NULL incentiveseq,
	NULL,
	NULL "Subline",
	NULL accountingdate,
	NULL "Order ID",
	NULL "Order Line Shipped Date",
	NULL "Compensation Date",
	NULL,
	NULL "Event Type",
	NULL "Native Amount",
	NULL "Native Currency",
	TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sinc.genericattribute4 , ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', '')) "Exchange Rate",
	NULL "Transaction Amount (USD)",
	NULL "Quota",
	NULL "TI (LOC)",
	NULL "Tier rate",
	NULL,
	sinc.value "Commission (LC)",
	h.local_currency "Commission Local Currency",
	(sinc.value * (TO_DOUBLE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sinc.genericattribute4 , ',', ''), '%', ''), 'USD', ''), 'quantity', ''), 'integer', ''))/100) ) "Commission (USD)",
	NULL,
	--bta.country,
 NULL "Bill to County",
	NULL,
	--bta.state,
 NULL "L8 Item Number",
	NULL "L2 Product",
	NULL "L3 Product",
	NULL "L4 Product",
	NULL "L5 Product",
	NULL "L6 Product",
	NULL "Product ID",
	NULL "Product Description",
	NULL "Customer ID",
	--bta.custid 
 NULL "Company",
	--bta.company 
 NULL "Invoice Line Number",
	NULL "Number of Units",
	NULL "Revenue Account",
	NULL "Plan Element",
	NULL comments,
	NULL "Customer PO Number",
	NULL "Order Line Booked Date"
FROM
	EXT.RPT_ORGDATA h,
	TCMP.cs_incentive sinc
WHERE
	h.processingunitseq = V_PUSEQ
	AND h.periodseq = V_PERIODSEQ
	AND IFNULL(sinc.genericboolean1, 0) = 1
	AND sinc.name IN ('IO Guarantee')
	AND h.periodseq = sinc.periodseq
	AND h.positionseq = sinc.positionseq;

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL procedure - Inserted Guarantee records into RPT_ACCRUALDETAIL_TMP table',
	3.2
FROM
	dummy ;

COMMIT;
-- updates BillTo related values
 UPDATE
	EXT.RPT_ACCRUALDETAIL_TMP ad
SET
	(BILLTO_COUNTRY,
	BILLTO_STATE,
	BILLTO_CUSTID,
	BILLTO_COMPANY) = (
	SELECT
		bta.country,
		bta.state,
		bta.custid,
		bta.company
	FROM
		tcmp.cs_transactionaddress bta,
		tcmp.cs_addresstype ta
	WHERE
		ta.ADDRESSTYPEID = 'BILLTO'
		AND ta.ADDRESSTYPESEQ = bta.ADDRESSTYPESEQ
		AND bta.salestransactionseq = ad.salestransactionseq);

COMMIT;

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL procedure - Updated RPT_ACCRUALDETAIL_TMP table for BillTo Values',
	3
FROM
	dummy ;

COMMIT;

INSERT
	INTO
	EXT.RPT_ACCRUALDETAIL
SELECT
	'Report Period',
	'ID',
	'Name',
	'Division',
	'Dept Code',
	'Credit Type',
	'Invoice Number',
	'Account Date',
	'Order ID',
	'Order Line Shipped Date',
	'Compensation Date',
	'Transaction Type',
	'Native Amount',
	'Native Currency',
	'Exchange Rate',
	'Transaction Amount (USD)',
	'Tier rate',
	'Commission (LC)',
	'Local Currency',
	'Commission (USD)',
	'Country',
	'County',
	'State',
	'L8 Item Number',
	'L2 Product',
	'L3 Product',
	'L4 Product',
	'L5 Product',
	'L6 Product',
	'Product ID',
	'Product Description',
	'Customer ID',
	'Customer Name',
	'Invoice Line Number',
	'Number of Units',
	'Revenue Account',
	'Plan Element',
	'Comments',
	'Customer PO Number',
	'Order Line Booked Date'
FROM
	dummy;

INSERT
	INTO
	EXT.RPT_ACCRUALDETAIL
SELECT
	PERIODNAME,
	PAYEEID,
	FULLNAME,
	DIVISION,
	DEPT_CODE,
	CREDIT_TYPE,
	SUBLINENUMBER,
	ACCOUNTINGDATE,
	ORDERID,
	ORDER_SHIP_DATE,
	COMPENSATIONDATE,
	EVENTTYPE,
	CASE WHEN PLAN_ELEMENT IN ('MBO','YOY Growth') THEN ROUND(NATIVECURRENCYAMOUNT,4)|| '%'
	ELSE TO_VARCHAR(ROUND(NATIVECURRENCYAMOUNT,4))
END ,
NATIVECURRENCY,
EXCHANGE_RATE,
CASE WHEN PLAN_ELEMENT IN ('MBO','YOY Growth') THEN round(TRXN_AMOUNT,4)|| '%'
ELSE '$' || round(TRXN_AMOUNT,4)
END ,
TIER_RATE,
ROUND(COMMISSION_LC, 4),
LOCAL_CURRENCY,
'$' || ROUND(COMMISSION_USD, 4),
BILLTO_COUNTRY,
BILLTO_COUNTY,
BILLTO_STATE,
L8_ITEM_NUMBER,
L2_PRODUCT,
L3_PRODUCT,
L4_PRODUCT,
L5_PRODUCT,
L6_PRODUCT,
PRODUCT_ID,
PRODUCT_DESC,
BILLTO_CUSTID,
BILLTO_COMPANY,
INVOICE_LINENO,
NUMBEROFUNITS,
REVENUE_ACCOUNT,
PLAN_ELEMENT,
COMMENTS,
CUSTOMER_PONO,
ORDER_BOOK_DATE
FROM
EXT.RPT_ACCRUALDETAIL_TMP
ORDER BY
FULLNAME,
PLAN_ELEMENT,
ORDERID;

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL procedure - adding data into Final table',
	4
FROM
	dummy ;

COMMIT;

V_PERIODNAME_NOSPACE := REPLACE(V_PERIODNAME, ' ', '_');

FILENAME := 'ACCRUAL_DETAIL_' || V_PERIODNAME_NOSPACE || '_' || to_Varchar(CURRENT_DATE,
'YYYYMMDD')|| '.csv';

INSERT
	INTO
	EXT.RPT_DEBUG
SELECT
	'0046',
	NOW(),
	'SP_RPT_ACCRUAL_DETAIL Procedure Completed ' || V_PERIODNAME_NOSPACE,
	5
FROM
	dummy ;

COMMIT;
END