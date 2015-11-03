#!/usr/bin/python2.7
#
# Interface for the assignement
#Author: Abhilash Owk


#READ ME: Replace the global variable DATA_FILE_PATH value with the location(path) where the test_data.dat is saved on your machine

import psycopg2

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'ratings'
DATA_FILE_PATH="c:\\temp2\\dds\\assignment1\\test_data.dat"


def getopenconnection(user='postgres', password='123456', dbname='test_dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
	target=open(DATA_FILE_PATH, 'r')
	a=target.readlines()
	conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"		
	con=psycopg2.connect(conn_string)
	cur=con.cursor()
	cur.execute("create table ratings (userid integer, movieid integer, rating real)")
	con.commit()
	for line in a:
		columns=line.split('::')
		cur.execute("INSERT INTO ratings VALUES(%s,%s,%s)",(columns[0], columns[1], columns[2]))
		con.commit()
	print "Data is loaded successfully into the SQL tables"


def rangepartition(ratingstablename, numberofpartitions, openconnection):
	if(numberofpartitions<1 or (isinstance(numberofpartitions,int)==False)):
		print "Number of partitions cannot be less than 1 or a decimal value"
	else:
		MaxRating=5.0
		TableRange=MaxRating/numberofpartitions
		conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"
		con=psycopg2.connect(conn_string)
		cur=con.cursor()
		cur.execute("Create table Partitions (partition integer,lowerbound real,upperbound real)")
		con.commit()
		count=0
		partition=0
		#print count
		#print TableRange
		print "Data Range Partitioning in progress"
		while(count<5):
			partition=partition+1
			conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"
			con=psycopg2.connect(conn_string)
			cur=con.cursor()
			#cur.execute("Create table range_part%s (userid integer, movieid integer, rating real)",(partition,))
			cur.execute("Create table range_part%s AS Select * from ratings where rating >=%s and rating < %s",(partition,count,count+TableRange,))
			cur.execute("Insert into partitions values(%s,%s,%s)",(partition, count,count+TableRange))
			con.commit()
			count=count+TableRange
			#TableRange=TableRange+TableRange
			#print count
			#print TableRange
		if(5-count>0):
			cur.execute("insert into range_part%s select * from ratings where rating=5",(partition,))
			con.commit()
		else:
			cur.execute("insert into range_part%s select * from ratings where rating=5",(partition,))
			con.commit()
		print "Data Range Partitioning is finished"
		
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
	if(numberofpartitions<1 or (isinstance(numberofpartitions,int)==False)):
		print "Number of partitions cannot be less than 1 or a decimal value"
	else:
		MaxRating=5.0
		TableRange=MaxRating/numberofpartitions
		conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"
		con=psycopg2.connect(conn_string)
		cur=con.cursor()
		cur.execute("Create table Partitions_Range (partition integer)")
		cur.execute("Insert into Partitions_Range values(%s)", (numberofpartitions,))
		con.commit()
		cur.execute("Select count(*) from ratings")
		Number_of_rows=cur.fetchone()
		count=0
		partition=0
		print "Round Robin Partitioning in progress"
		while(count<numberofpartitions):
			partition=partition+1
			conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"
			con=psycopg2.connect(conn_string)
			cur=con.cursor()
			#cur.execute("Create table rrobin_part%s (userid integer, movieid integer, rating real)",(partition,))
			cur.execute("Create table rrobin_part%s (userid integer, movieid integer, rating real)",(partition,))
			con.commit()
			count=count+1
		
		cur.execute("select * from ratings")
		Total_rows=cur.fetchall()
		partition=0
		for row in Total_rows:
			if partition<numberofpartitions:
				partition=partition+1
			else:
				partition=1
			cur.execute("Insert into rrobin_part%s values(%s,%s,%s)",(partition,row[0],row[1],row[2]))
			con.commit()
			
		print "Round Robin partitioning is finished"

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
	if(rating<0 or rating>5):
		print "Invalid ratings. Rating should range between 0 and 5, including 0 and 5"
	else:
		conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"
		con=psycopg2.connect(conn_string)
		cur=con.cursor()
		cur.execute("select count(*) from ratings")
		temp=cur.fetchone()
		no_of_rows=temp[0]
		cur.execute("select * from Partitions_Range")
		temp1=cur.fetchone()
		no_of_partitions=temp1[0]
		table_no=(no_of_rows+1) % no_of_partitions
		cur.execute("Insert into rrobin_part%s values(%s,%s,%s)",(table_no,userid,itemid,rating))
		cur.execute("Insert into ratings values(%s,%s,%s)",(userid,itemid,rating))
		con.commit()
		print "New insert successfully entered into the partition"


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
	if(rating<0 or rating>5):
		print "Invalid ratings. Rating should range between 0 and 5, including 0 and 5"
	else:
		conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"
		con=psycopg2.connect(conn_string)
		cur=con.cursor()
		if(rating<5):
			cur.execute("select partition from partitions where lowerbound<=%s and upperbound>%s",(rating,rating))
			con.commit()
			Load_Partition=cur.fetchone()
			print Load_Partition[0]
			cur.execute("Insert into range_part%s values(%s,%s,%s)",(Load_Partition[0],userid,itemid,rating,))
			con.commit()
		else:
			cur.execute("select partition from partitions where upperbound=5")
			con.commit()
			Load_Partition=cur.fetchone()
			print Load_Partition[0]
			cur.execute("Insert into range_part%s values(%s,%s,%s)",(Load_Partition[0],userid,itemid,rating,))
			con.commit()
		print "New insert successfully entered into the partition"
		
def deletepartitionsandexit(openconnection):
	conn_string = "host='localhost' dbname='test_dds_assgn1' user='postgres' password='123456'"
	con=psycopg2.connect(conn_string)
	cur=con.cursor()
	cur.execute("select count(*) from partitions")
	con.commit()
	partition_value=cur.fetchone()
	part=partition_value[0]
	#print part
	#print "done1"
	for i in range(0,part+1):
		cur.execute("drop table if exists range_part%s",(i,))
		con.commit()
	cur.execute("drop table if exists partitions")
	con.commit()
		
	#print "done2"
	cur.execute("select * from Partitions_Range")
	temp1=cur.fetchone()
	no_of_partitions=temp1[0]
	for j in range(0,no_of_partitions+1):
		cur.execute("drop table if exists rrobin_part%s",(j,))
		con.commit()
	cur.execute("drop table if exists partitions_range")
	con.commit()
	print "Done"
		


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
		print "creating"
		cur.execute('CREATE DATABASE %s' % (dbname,))
	
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
	pass
def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass
	



if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
			before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
			loadratings(RATINGS_TABLE,DATA_FILE_PATH, con)
			
			rangepartition(RATINGS_TABLE, 10, con)
			rangeinsert(RATINGS_TABLE, 2,34,4,con)
			rangeinsert(RATINGS_TABLE, 6,340,5,con)
			roundrobinpartition(RATINGS_TABLE,5,con)
			roundrobininsert(RATINGS_TABLE, 9,9,4,con)
			deletepartitionsandexit(con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
			after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
