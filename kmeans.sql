drop table if exists dataset;
create table if not exists dataset(p int,x float,y float);
insert into dataset values(1,1,1),(2,1.5,2),(3,3,4),(4,5,7),(5,3.5,5),(6,4.5,5),(7,3.5,4.5);

create or replace function kmeans(k int)
returns table(x float,y float) as
$$
declare
    n int;
begin
    drop table if exists centroids;
    drop table if exists temp;
    drop table if exists tempgroups;
    drop table if exists tempgroups2;
    drop table if exists temp2;
    create table centroids(no int,x float,y float);
    for n in 1..k
    loop
        insert into centroids values(n,n,n);
    end loop;
    perform groups(k);
    return query select c.x,c.y from centroids c;
end;
$$ language plpgsql;

create or replace function distance(p1 int,grp int)
returns float as
$$
declare
    x1 float;
    y1 float;
    x2 float;
    y2 float;
    d float;
begin
        select into x1 x from dataset where p=p1;
        select into y1 y from dataset where p=p1;
        select into x2 x from centroids where no=grp;
        select into y2 y from centroids where no=grp;
        d:=||/((x1-x2)^2+(y1-y2)^2);
        return d;
end;
$$ language plpgsql;

create or replace function centroid(k int)
returns void as
$$
declare
    n int;
    r record;
    xsum float;
    ysum float;
    xcount int;
    ycount int;
    x1 float;
    y1 float;
    xavg float;
    yavg float;
begin
    drop table if exists tempp;
    create table tempp(a int);
    drop table if exists temp2;
    create table temp2(grp int,sumx float,countx int,sumy float,county int,avgx float,avgy float);
    for n in 1..k
    loop
        insert into temp2 values(n,0,0,0,0,0,0);
        for r in select * from tempgroups where tempgroups.grp=n
        loop
            select into xsum sumx from temp2 where grp=n;
            select into ysum sumy from temp2 where grp=n;
            select into xcount countx from temp2 where grp=n;
            select into ycount county from temp2 where grp=n;
            select into x1 x from dataset where dataset.p=r.p;
            select into y1 y from dataset where dataset.p=r.p;
            xsum=xsum+x1;
            ysum=ysum+y1;
            xcount=xcount+1;
            ycount=ycount+1;
            xavg=xsum/xcount;
            yavg=ysum/ycount;
            update temp2 set sumx=xsum,countx=xcount,sumy=ysum,county=ycount,avgx=xavg,avgy=yavg where grp=n;
        end loop;
    end loop;
    for r in select * from temp2
    loop
        update centroids set x=r.avgx,y=r.avgy where no=r.grp;
    end loop;
end;
$$ language plpgsql;

create or replace function groups(k int)
returns void as
$$
declare
    r record;
    n int;
    d float;
begin
    drop table if exists temp;
    drop table if exists tempgroups;
    create table temp(p int,grp int,dist float);
    create table tempgroups(p int,grp int);
    create table if not exists tempcent(no int,x float,y float);
    for r in select * from dataset
    loop
        for n in 1..k
        loop
            select into d distance(r.p,n);
            insert into temp values(r.p,n,d);
        end loop;
        insert into tempgroups select temp.p,temp.grp from temp where temp.p=r.p and dist<=all(select dist from temp where p=r.p);
    end loop;
    perform centroid(k);
    if not exists(select * from centroids except select * from tempcent)
        then return;
    else
        delete from tempcent;
        insert into tempcent select * from centroids;
        perform groups(k);
    end if;
end;
$$ language plpgsql;

select kmeans(2);
