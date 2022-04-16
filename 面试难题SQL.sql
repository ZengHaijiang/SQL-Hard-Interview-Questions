/*近N日留存*/
/*device_id, event_time*/
select
	log_day '日期',
	count(user_id_d0) '新增数量',
	count(user_id_d1) / count(user_id_d0) '次日留存率',
	count(user_id_d3) / count(user_id_d0) '3日留存率',
	count(user_id_d7) / count(user_id_d0) '7日留存率',
from (
	select 
		distinct log_day,
		a.user_id_d0,
		b.device_id as user_id_d1,
		c.device_id as user_id_d3,
		d.device_id as user_id_d7
	from 
		(select 
			distinct date(event_time) as log_day, # 只关心日期，不关注具体的时间。
			device_id as user_id_d0
		from role_login_back
		group by device_id
		order by log_day) a
	left join role_login_back b 
	on datediff(date(b.event_time),a.log_day) = 1 
	and a.user_id_d0 = b.device_id
	left join role_login_back c 
	on datediff(date(c.event_time), a.log_day) = 2
	and a.user_id_d0 = c.device_id
	left join role_login_back d
	on datediff(date(d.event_time), a.log_day) = 6
	and a.user_id_d0 = d.device_id 
	)
group by log_day;

/*连续N天登录*/
/*user_id, login_date*/
/*连续登录的diff_date（日期-排序）是一致的*/
select
    user_id 
    ,max(count(diff_date))
from(
    select
        user_id, login_date,
        date_sub(login_date, rn) diff_date  -- 第三步
    from(
        select
            user_id, login_date,
            row_number() over(partition by user_id order by login_date) rn  -- 第二步
        from(select distinct user_id, substr(login_time, 0, 10) as date from login_info)t  -- 第一步
        )tt
    )ttt
group by user_id

/*最大登录人数*/
/*live_id,user_id,login_time,logout_time,dt*/
/*把列拆成行，有一个变量在线人数能够每次login就+1，logout就-1*/
select t2.live_id,  max(t2.cnt) as max_online_cnt
from
   (select t1.live_id, t1.time, sum(t1.indexx) over(partition by t1.live_id order by t1.time) as cnt
    from
       (select live_id, login_time as time, 1 as indexx from live_table where dt = '20210801'
        union all
        select live_id, logout_time as time, -1 as indexx from live_table where dt = '20210801'
        ) t1 
    ) t2
where t1.time >=12 and t1.time <=13
group by t2.live_id



