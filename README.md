# 카카오페이 사전과제
> 비즈니스분석가 사전과제

### 문제 3)

* 데이터 준비
<pre><code>
/* 데이터 업로드 */

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


/* 데이터 확인 */

SELECT SUM(1) FROM share_mns.lhj_x_users;								--28407
SELECT SUM(1) FROM share_mns.lhj_x_product;							--298
SELECT SUM(1) FROM share_mns.lhj_x_transaction_history;	--380782
SELECT SUM(1) FROM share_mns.lhj_dutchpay_claim_tx;			--982871
SELECT SUM(1) FROM share_mns.lhj_dutchpay_claim_detail;	--3413602
</code></pre>

