# 카카오페이 사전과제
> 비즈니스분석가 사전과제
   
   
## 문제 3번

### 1. 데이터 업로드
<pre><code>
-- 1) x_users.csv : 서비스 X의 유저 데이터

drop table share_mns.lhj_x_users;
create table share_mns.lhj_x_users
(
uid string
,gender_cd	string
,age	int

)
row format delimited fields terminated by ','
  lines terminated by '\n'
  stored as textfile
;

>$ /app/hdfs/bin/hadoop fs -copyFromLocal /home/pp61123/upload/lhj_x_users.csv /user/hive/warehouse/share_mns.db/lhj_x_users 


-- 2) x_product.csv : 서비스 X에서 제공하는 상품 데이터

drop table share_mns.lhj_x_product;
create table share_mns.lhj_x_product
(
pid string
,product_end_at	string
,info1	string
,info2	string

)
row format delimited fields terminated by ','
  lines terminated by '\n'
  stored as textfile
;

>$ /app/hdfs/bin/hadoop fs -copyFromLocal /home/pp61123/upload/lhj_x_product.csv /user/hive/warehouse/share_mns.db/lhj_x_product 


-- 3) x_transaction_history.csv : 서비스 X의 트랜잭션 이력 데이터

drop table share_mns.lhj_x_transaction_history;
create table share_mns.lhj_x_transaction_history
(
tid string
,uid	string
,pid	string
,channel	string
,status	string
,amount	int
,created_at	string

)
row format delimited fields terminated by ','
  lines terminated by '\n'
  stored as textfile
;

>$ /app/hdfs/bin/hadoop fs -copyFromLocal /home/pp61123/upload/lhj_x_transaction_history.csv /user/hive/warehouse/share_mns.db/lhj_x_transaction_history 


-- 4) dutchpay_claim_tx.csv : 더치페이 요청 테이블

drop table share_mns.lhj_dutchpay_claim_tx;
create table share_mns.lhj_dutchpay_claim_tx
(
id string
,created_at	string
,user_id	string

)
row format delimited fields terminated by ','
  lines terminated by '\n'
  stored as textfile
;


>$ /app/hdfs/bin/hadoop fs -copyFromLocal /home/pp61123/upload/lhj_dutchpay_claim_tx.csv /user/hive/warehouse/share_mns.db/lhj_dutchpay_claim_tx 


-- 5) dutchpay_claim_detail.csv : 더치페이 요청 상세 테이블

drop table share_mns.lhj_dutchpay_claim_detail;
create table share_mns.lhj_dutchpay_claim_detail
(
id string
,created_at	string
,remittance_claim_send_id	string
,user_id	string
,claim_amount	int
,send_amount	int
,status	string
,send_transaction_event_id	string

)
row format delimited fields terminated by ','
  lines terminated by '\n'
  stored as textfile
;

>$ /app/hdfs/bin/hadoop fs -copyFromLocal /home/pp61123/upload/lhj_dutchpay_claim_detail.csv /user/hive/warehouse/share_mns.db/lhj_dutchpay_claim_detail  


-- 데이터 확인
SELECT SUM(1) FROM share_mns.lhj_x_users; --28407
SELECT SUM(1) FROM share_mns.lhj_x_product; --298
SELECT SUM(1) FROM share_mns.lhj_x_transaction_history; --380782
SELECT SUM(1) FROM share_mns.lhj_dutchpay_claim_tx; --982871
SELECT SUM(1) FROM share_mns.lhj_dutchpay_claim_detail; --3413602
</code></pre>


### 2. 데이터 추출
<pre><code>-- 3-1) 누적 리워드 지급 10회 이상인 유저
SELECT COUNT(DISTINCT UID)
 FROM 
(SELECT UID
	,COUNT(DISTINCT CASE WHEN AMOUNT >= 100000 THEN SUBSTR(CREATED_AT,1,10) ELSE NULL END) AS RWD_CNT
   FROM share_mns.lhj_x_transaction_history
  WHERE STATUS = 'COMPLETED'
  GROUP BY UID
 ) AA
 WHERE RWD_CNT >= 10
;
1407

-- 3-2) 누적 리워드 지급 5회 이상인 유저(3-1 대상 유저와 중복 제거)
SELECT COUNT(DISTINCT UID)
FROM 
(SELECT UID
	,COUNT(DISTINCT CASE WHEN AMOUNT >= 100000 THEN SUBSTR(CREATED_AT,1,10) ELSE NULL END) AS RWD_CNT
FROM share_mns.lhj_x_transaction_history
 WHERE STATUS = 'COMPLETED'
GROUP BY UID
) AA
 WHERE RWD_CNT BETWEEN 5 AND 9
;
2449
</code></pre>
--------------------------------------

## 문제 4번

### 1. R 코드
<pre><code>
#csv파일 불러오기
setwd("d:/")
dutchpay_claim_detail<-read.csv("dutchpay_claim_detail.csv",header=T)
dutchpay_claim_tx<-read.csv("dutchpay_claim_tx.csv",header=T)
x_product<-read.csv("x_product.csv",header=T)
x_transaction_history<-read.csv("x_transaction_history.csv",header=T)
x_users<-read.csv("x_users.csv",header=T)


# SQLDF 함수를 사용하기 위한 라이브러리
library(sqldf)

# SQLite 관련 함수를 사용하기 위한 라이브러리
library(RSQLite)

# 요청한 고객의 요청수, 응답수
claim_user<-sqldf("
                  select claim_uid
                        ,count(distinct claim_id) as claim_cnt
                        ,count(distinct case when send_yn=1 then claim_id end) as send_cnt
                    from (select aa.user_id as claim_uid
                                 ,aa.id      as claim_id
                                 ,max(case when bb.status='SEND' then 1 else 0 end) as send_yn
                             from dutchpay_claim_tx aa
                             inner join dutchpay_claim_detail bb
                                     on aa.id = bb.remittance_claim_send_id
                           group by aa.user_id, aa.id
                          ) aaa
                  group by claim_uid
                  ")

# 요청받은 고객의 응답수(서비스 사용수)
send_user<-sqldf("select user_id as send_uid
                        ,count(distinct SEND_TRANSACTION_EVENT_ID) as use_cnt
                    from dutchpay_claim_detail
                  group by user_id
                  ")

# 고객별 응답률, 서비스 사용수
anal_table<-sqldf("select claim_uid
                         ,(aa.send_cnt+0.00)/(aa.claim_cnt+0.00)  as send_rt
                         ,bb.use_cnt
                    from claim_user aa
                      left join send_user bb
                             on aa.claim_uid = send_uid")

# 고객별 응답률, 서비스 사용수의 상관분석
cor.test(anal_table$send_rt,anal_table$use_cnt)
</code></pre>


### 2. 분석 결과
<pre><code>
	Pearson's product-moment correlation

data:  anal_table$send_rt and anal_table$use_cnt
t = 100.12, df = 315681, p-value < 2.2e-16
alternative hypothesis: true correlation is not equal to 0
95 percent confidence interval:
 0.1720569 0.1788189
sample estimates:
    cor 
0.17544
</code></pre>


### 2. 분석 결과
<pre><code>
피어슨 상관분석 결과, p-value가 0.05보다 작으므로 두 변수간의 상관이 없다는 귀무가설을 기각한다.
하지만 상관계수가 0.18로 매우 약한 관련성을 가진다.
</code></pre>
-----------------------------------------

## 문제 5번

### 1. 데이터 추출
* 테이블은 문제 3번에서 업로드한 테이블로 사용.

<pre><code>-- 사용자의 더치페이 재사용률을 분석
SELECT CASE WHEN FST_DT >= '20191201' AND FST_DT <= '20191214' THEN '1.12/01_12/14'
	    WHEN FST_DT >= '20191215' AND FST_DT <= '20191228' THEN '2.12/15_12/28'
	    WHEN FST_DT >= '20191229' AND FST_DT <= '20200104' THEN '3.12/29_01/04'
	    WHEN FST_DT >= '20200105' AND FST_DT <= '20200118' THEN '4.01/05_01/18'
	    WHEN FST_DT >= '20200119' AND FST_DT <= '20200201' THEN '5.01/19_02/01'
	    WHEN FST_DT >= '20200202' AND FST_DT <= '20200215' THEN '6.02/02_02/15'
	    WHEN FST_DT >= '20200216' AND FST_DT <= '20200229' THEN '7.02/16_02/29' END AS FST_DT
     ,COUNT(DISTINCT AA.USER_ID)	AS BASE_DNT
     ,COUNT(DISTINCT CASE WHEN BB.CRE_DT > FST_DT AND BB.CRE_DT <= AA.AF_2W THEN AA.USER_ID END)  AS AF_2W_DNT
     ,COUNT(DISTINCT CASE WHEN BB.CRE_DT > FST_DT AND BB.CRE_DT <= AA.AF_4W THEN AA.USER_ID END)  AS AF_4W_DNT
     ,COUNT(DISTINCT CASE WHEN BB.CRE_DT > FST_DT AND BB.CRE_DT <= AA.AF_6W THEN AA.USER_ID END)  AS AF_6W_DNT
     ,COUNT(DISTINCT CASE WHEN BB.CRE_DT > FST_DT AND BB.CRE_DT <= AA.AF_8W THEN AA.USER_ID END)  AS AF_8W_DNT
     ,COUNT(DISTINCT CASE WHEN BB.CRE_DT > FST_DT AND BB.CRE_DT <= AA.AF_10W THEN AA.USER_ID END) AS AF_10W_DNT
     ,COUNT(DISTINCT CASE WHEN BB.CRE_DT > FST_DT AND BB.CRE_DT <= AA.AF_12W THEN AA.USER_ID END) AS AF_12W_DNT
     ,COUNT(DISTINCT CASE WHEN BB.CRE_DT > FST_DT AND BB.CRE_DT <= AA.AF_14W THEN AA.USER_ID END) AS AF_14W_DNT
 FROM (
      SELECT USER_ID
	    ,REGEXP_REPLACE(FST_DT,'-','') AS FST_DT
	    ,REGEXP_REPLACE(DATE_ADD(from_unixtime(unix_timestamp(FST_DT,'yyyy-MM-dd')),14),'-','') AS AF_2W	--2주 후
	    ,REGEXP_REPLACE(DATE_ADD(from_unixtime(unix_timestamp(FST_DT,'yyyy-MM-dd')),28),'-','') AS AF_4W	--4주 후
	    ,REGEXP_REPLACE(DATE_ADD(from_unixtime(unix_timestamp(FST_DT,'yyyy-MM-dd')),42),'-','') AS AF_6W	--6주 후
	    ,REGEXP_REPLACE(DATE_ADD(from_unixtime(unix_timestamp(FST_DT,'yyyy-MM-dd')),56),'-','') AS AF_8W	--8주 후
	    ,REGEXP_REPLACE(DATE_ADD(from_unixtime(unix_timestamp(FST_DT,'yyyy-MM-dd')),70),'-','') AS AF_10W	--10주 후
	    ,REGEXP_REPLACE(DATE_ADD(from_unixtime(unix_timestamp(FST_DT,'yyyy-MM-dd')),84),'-','') AS AF_12W	--12주 후
	    ,REGEXP_REPLACE(DATE_ADD(from_unixtime(unix_timestamp(FST_DT,'yyyy-MM-dd')),98),'-','') AS AF_14W	--14주 후
	FROM (
	      SELECT USER_ID, MIN(SUBSTR(CREATED_AT,1,10)) AS FST_DT	--최초 요청일자
		FROM share_mns.lhj_dutchpay_claim_tx
	      GROUP BY USER_ID
	     ) A
	)	AA
	LEFT OUTER JOIN (SELECT DISTINCT USER_ID
			       ,REGEXP_REPLACE(SUBSTR(CREATED_AT,1,10),'-','') AS CRE_DT
			  FROM share_mns.lhj_dutchpay_claim_tx
			) BB
		     ON AA.USER_ID = BB.USER_ID
GROUP BY CASE WHEN FST_DT >= '20191201' AND FST_DT <= '20191214' THEN '1.12/01_12/14'
	      WHEN FST_DT >= '20191215' AND FST_DT <= '20191228' THEN '2.12/15_12/28'
	      WHEN FST_DT >= '20191229' AND FST_DT <= '20200104' THEN '3.12/29_01/04'
	      WHEN FST_DT >= '20200105' AND FST_DT <= '20200118' THEN '4.01/05_01/18'
	      WHEN FST_DT >= '20200119' AND FST_DT <= '20200201' THEN '5.01/19_02/01'
	      WHEN FST_DT >= '20200202' AND FST_DT <= '20200215' THEN '6.02/02_02/15'
	      WHEN FST_DT >= '20200216' AND FST_DT <= '20200229' THEN '7.02/16_02/29' END
;
</code></pre>


### 2. 분석 결과

![재방문률](https://user-images.githubusercontent.com/7845923/74233648-6e156c00-4d0e-11ea-8f11-f8d3511bc06d.PNG)
