with q1_count_match as ( -- ile meczy
                         SELECT 
                         master,
                         league,
                         count(*) as Countofmatches
                         FROM 
                         dataset1.data
                         group by 
                         league,
                         master),

q2_FT as (--naczęstszy wynik FT
          select * from (
            select 
            a.*,
            rank () OVER (
              PARTITION BY 
              master,
              league
              ORDER BY
              FT2 desc) as rank
            from (
              SELECT 
              master,
              league,
              FT,
              count(*) as FT2 -- potrzebne do zliczania FT
              FROM 
              dataset1.data
              where regexp_replace(FT,'[[:digit:]]{1,2}\\-[[:digit:]]{1,2}','') = ''
              group by 
              league,
              master,
              FT) a) a 
          where rank = 1),

q3_HT as (--naczęstszy wynik HT
          select * from (
            select 
            a.*,
            rank () OVER (
              PARTITION BY 
              master,
              league
              ORDER BY
              HT2 desc) as rank
            from (
              SELECT 
              master,
              league,
              HT,
              count(*) as HT2
              FROM 
              dataset1.data
              where regexp_replace(HT,'[[:digit:]]{1,2}\\-[[:digit:]]{1,2}','') = ''
              group by 
              league,
              master,
              HT) a) a 
          where rank = 1),

q4a_ileZwyciestw as ( -- zliczenie ile zwycięstw miała poszczególna drużyna
                      select
                      a.master,
                      a.league,
                      case --ustalenie która drużyna wygrała
                      when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team1,' \\([[:digit:]]{0,2}\\)','') 
                      when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team2,' \\([[:digit:]]{0,2}\\)','') 
                      when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)=cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric)  then '-' else 'not avalible' end as WIN_TEAM,
                      count(*) as ILE_ZWYC,
                      rank () OVER (
                        PARTITION BY 
                        a.master,
                        a.league
                        ORDER BY
                        count(*) desc) as rank
                      FROM 
                      dataset1.data a
                      where regexp_replace(FT,'[[:digit:]]{1,2}\\-[[:digit:]]{1,2}','') = '' and --usunięcie zbednych wpisów w wyniakch innyniż 1-0, 2-1 itp.
                      regexp_replace(FT,'\\-[[:digit:]]{1,2}','')<>regexp_replace(FT,'[[:digit:]]{1,2}\\-','')
                      group by 
                      a.master,
                      a.league,
                      case 
                      when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team1,' \\([[:digit:]]{0,2}\\)','') 
                      when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team2,' \\([[:digit:]]{0,2}\\)','') 
                      when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)=cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric)  then '-' else 'not avalible' end),

q4b_ileZwyciestw_ranked1 as ( -- wybranie najlepszych drużyn wg zwycięstw
                              select * from q4a_ileZwyciestw
                              where rank = 1),

q4b_ileZwyciestw_ranked as (-- sprawdzenie czy jest jedna drużyna
                            select * from q4b_ileZwyciestw_ranked1
                            where concat(master,league) in (select concat(master,league) from q4b_ileZwyciestw_ranked1  group by concat(master,league) having sum(rank) = 1)),

q5a_ileZwyciestw_HT as (  -- zliczenie ile zwycięstw miała poszczególna drużyna HT
                          select
                          a.master,
                          a.league,
                          case --ustalenie która drużyna wygrała HT
                          when cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)>cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team1,' \\([[:digit:]]{0,2}\\)','') 
                          when cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)<cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team2,' \\([[:digit:]]{0,2}\\)','') 
                          when cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)=cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric)  then '-' else 'not avalible' end as WIN_TEAM,
                          count(*) as ILE_ZWYC,
                          rank () OVER (
                            PARTITION BY 
                            a.master,
                            a.league
                            ORDER BY
                            count(*) desc) as rank
                          FROM 
                          dataset1.data a
                          where regexp_replace(HT,'[[:digit:]]{1,2}\\-[[:digit:]]{1,2}','') = ''  and  --usunięcie zbednych wpisów w wyniakch innyniż 1-0, 2-1 itp.
                          regexp_replace(HT,'\\-[[:digit:]]{1,2}','') <>regexp_replace(HT,'[[:digit:]]{1,2}\\-','')--usunięcie remisów
                          group by 
                          a.master,
                          a.league,
                          case 
                          when cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)>cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team1,' \\([[:digit:]]{0,2}\\)','') 
                          when cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)<cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team2,' \\([[:digit:]]{0,2}\\)','') 
                          when cast(regexp_replace(HT,'\\-[[:digit:]]{1,2}','') as numeric)=cast(regexp_replace(HT,'[[:digit:]]{1,2}\\-','') as numeric)  then '-' else 'not avalible' end),

q5b_ileZwyciestw_HT_ranked1 as -- wybranie najlepszych drużyn wg zwycięstw HT
(select * from q5a_ileZwyciestw_HT
  where rank = 1),

q5b_ileZwyciestw_HT_ranked as ( -- sprawdzenie czy jest jedna drużyna HT
                                select * from q5b_ileZwyciestw_HT_ranked1
                                where concat(master,league) in (select concat(master,league) from q5b_ileZwyciestw_HT_ranked1  group by concat(master,league) having sum(rank) = 1)),

q6a_najwyzsza_przewaga as ( --sprawdzenie drużyn, które mają najwyższy wynik liczone jako największa przewaga
                            select 
                            master,
                            league,
                            string_agg(Date, '&') as Date, --agregacja daty meczu dla drużyny o najwyższym wyniku
                            WIN_TEAM,
                            ROZNICA,
                            rank
                            from(
                              select
                              master,
                              league,
                              regexp_replace(Date,' \\([[:digit:]]{0,2}\\)','') as Date, -- usuniecie numeru tygodnia np (33)
                              case 
                              when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team1,' \\([[:digit:]]{0,2}\\)','') 
                              when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team2,' \\([[:digit:]]{0,2}\\)','') 
                              when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)=cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then '-' else 'not avalible' end WIN_TEAM,
                              ABS(cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)-cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric)) as ROZNICA, -- wartość bezwzględna mięczy różnicą z wyniku
                              FT,
                              rank () OVER (
                                PARTITION BY 
                                master,
                                league
                                ORDER BY
                                ABS(cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)-cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric)) desc) as rank
                              FROM 
                              dataset1.data a
                              where regexp_replace(FT,'[[:digit:]]{1,2}\\-[[:digit:]]{1,2}','') = '' --usunięcie zbednych wpisów w wyniakch innych niż 1-0, 2-1 itp.
                              group by
                              master,
                              league,
                              FT,
                              regexp_replace(Date,' \\([[:digit:]]{0,2}\\)',''),
                              ABS(cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)-cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric)) ,
                              case 
                              when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team1,' \\([[:digit:]]{0,2}\\)','') 
                              when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) and cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)<>cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric) then regexp_replace(Team2,' \\([[:digit:]]{0,2}\\)','') 
                              when cast(regexp_replace(FT,'\\-[[:digit:]]{1,2}','') as numeric)=cast(regexp_replace(FT,'[[:digit:]]{1,2}\\-','') as numeric)  then '-' else 'not avalible' end) a
                            where rank = 1
                            group by 
                            master,
                            league,
                            WIN_TEAM,
                            ROZNICA,
                            rank),

q6b_najwyzsza_przewaga_ranked as ( --wybranie tylko jednej drużyny o najwyższym wyniki
                                   select * from q6a_najwyzsza_przewaga
                                   where concat(master,league) in (select concat(master,league) from q6a_najwyzsza_przewaga  group by concat(master,league) having sum(rank) = 1)),

q7a_najwiecej_meczy as( -- obliczenie liczby meczy dla poszczególnych drużyn
                        select * from (
                          select 
                          master,
                          league,
                          Team1,
                          count(*) as ILE_MECZY,
                          rank () OVER (
                            PARTITION BY 
                            master,
                            league
                            ORDER BY
                            count(*) desc) as rank
                          from (
                            select master,
                            league,
                            regexp_replace(Team1,' \\([[:digit:]]{0,2}\\)','')  as Team1
                            from dataset1.data
                            union all
                            select master,
                            league,
                            regexp_replace(Team2,' \\([[:digit:]]{0,2}\\)','')  as Team2
                            from 
                            dataset1.data) a
                          group by 
                          master,
                          league,
                          Team1)a 
                        where rank=1),

q7b_najwiecej_meczy_ranked as ( -- wybranie drużyny o najwyższym wyniku, jeżeli jest tylko jedna taka drużyna
                                select * from q7a_najwiecej_meczy
                                where concat(master,league) in (select concat(master,league) from q7a_najwiecej_meczy  group by concat(master,league) having sum(rank) = 1))

select 
--q1.master as country,
case 
when 
concat(upper(substr(regexp_replace(regexp_replace(q1.master,'\\-master',''),'[[:alpha:]]{1,3}\\-',''),0,1)),substr(
  regexp_replace(regexp_replace(q1.master,'\\-master',''),'[[:alpha:]]{1,3}\\-',''),2)) = 'Deutschland' then 'Germany'
when 
concat(upper(substr(regexp_replace(regexp_replace(q1.master,'\\-master',''),'[[:alpha:]]{1,3}\\-',''),0,1)),substr(
  regexp_replace(regexp_replace(q1.master,'\\-master',''),'[[:alpha:]]{1,3}\\-',''),2)) = 'Espana' then 'Spain'
else 
concat(upper(substr(regexp_replace(regexp_replace(q1.master,'\\-master',''),'[[:alpha:]]{1,3}\\-',''),0,1)),substr(
  regexp_replace(regexp_replace(q1.master,'\\-master',''),'[[:alpha:]]{1,3}\\-',''),2))  end as country,
--q1.league,
regexp_replace(q1.league,'.csv','') as league,
q2.FT as modewinningscoreFT,
q3.HT as modewinningscoreHT,
q4.WIN_TEAM as modewinningteamFT,
q5.WIN_TEAM as modewinningteamHT,
q6b.WIN_TEAM as highestdominationteam, 
q6b.Date as highestdominationdate,
q7b. Team1 as teamhighestcountofmatchesTeam,
q1.Countofmatches
from
q1_count_match q1
left join q2_FT q2 on (q1.master = q2.master and q1.league = q2.league)
left join q3_HT q3 on (q1.master = q3.master and q1.league = q3.league)
left join q4b_ileZwyciestw_ranked q4 on (q1.master = q4.master and q1.league = q4.league)
left join q5b_ileZwyciestw_HT_ranked q5 on (q1.master = q5.master and q1.league = q5.league)
left join q6b_najwyzsza_przewaga_ranked q6b on (q1.master = q6b.master and q1.league = q6b.league)
left join q7b_najwiecej_meczy_ranked q7b on (q1.master = q7b.master and q1.league = q7b.league)
order by 1,2
;
