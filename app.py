# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import settings
import sys,os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql as db

def connection():
    con = db.connect(
    host=settings.mysql_host,
    user=settings.mysql_user,
    password=settings.mysql_passwd,
    database=settings.mysql_schema)
    return con
#findTrips good to go!!!
def  findTrips(x,a,b):
    # Create a new connection
    con=connection()
    cur=con.cursor()
    #code

    #first sql query to get a table(table1) containg trip_package_id to be used as an identifier for the merger,
    # cost_per_person,max_num_participants, c which is the total reservations of this branch for each trip and trip_start and trip_end,
    #according to the parameters given

    sql1="""select trip_package_id,cost_per_person,max_num_participants, count(trip_package_id) as c, trip_start, trip_end
from trip_package, reservation, travel_agency_branch
where offer_trip_package_id=trip_package.trip_package_id and reservation.travel_agency_branch_id=travel_agency_branch.travel_agency_branch_id
and trip_start>=%s and trip_start<=%s and travel_agency_branch.travel_agency_branch_id=%s
group by trip_package_id, cost_per_person,max_num_participants, trip_start, trip_end
order by trip_package_id;"""
    cur.execute(sql1, (a, b, x))
    result1=cur.fetchall()

    #The second sql query to get a table(table2) that contains trip_package_id to be used as an identifier for thr merger 
    # and the surnames and names of a each guide according to the dates given in the parameters

    sql2="""select distinct trip_package.trip_package_id, employees.surname, employees.name
from trip_package, employees, guided_tour
where employees_AM=travel_guide_employee_AM and guided_tour.trip_package_id=trip_package.trip_package_id
and trip_start>=%s and trip_start<=%s
order by trip_package.trip_package_id;"""
    cur.execute(sql2, (a, b))
    result2=cur.fetchall()

    res = []
    for row1 in result1:
        identifier = row1[0]                                #trip_package_id used as an identifier
        data = row1[1:]                                    #taking all the data from table1 except for trip_package_id
        guide_names = []
        for row2 in result2:
            if row2[0] == identifier:                       #matching the guides to the correct trip_package
                surname = row2[1]
                name = row2[2]
                fullname = surname + ' ' + name             #merging the surnames and names of each guide
                guide_names.append(fullname)                #inserting them into the guide_names list
        max_num_participants = row1[2]
        reservations = row1[3]
        empty_seats = int(max_num_participants) - int(reservations)         #calculating the number of empty seats
        #inserting the data to the resulting list "res" according to the wanted order and checking if guide_names list is empty for
        #this row,if it is , it just leaves the space empty
        res.append(( *data[:3], empty_seats, ','.join(guide_names) if guide_names else '', *data[3:]))

    return [('Cost Per Person', 'Max Num Participants', 'Reservations', 'Empty Seats', 'Guide Names', 'Trip Start', 'Trip End')] + res


def findRevenue(x):
    
    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    c = con.cursor()
    # Creating table with the total_num_reservations
    sql ="""SELECT TAB.travel_agency_branch_id, count(1)
            FROM travel_agency_branch TAB, reservation RES
            WHERE RES.travel_agency_branch_id=TAB.travel_agency_branch_id
            GROUP BY TAB.travel_agency_branch_id
            ORDER BY TAB.travel_agency_branch_id"""
    c.execute(sql)
    num_res = c.fetchall()
        
#-----------TABLE WITH REVENUE------------------------------
    sql ="""SELECT TAB.travel_agency_branch_id, sum(offer.cost)
            FROM travel_agency_branch TAB, reservation RES, offer
            WHERE TAB.travel_agency_branch_id=RES.travel_agency_branch_id
            and RES.offer_id=offer.offer_id
            GROUP BY TAB.travel_agency_branch_id
            ORDER BY TAB.travel_agency_branch_id"""
    c.execute(sql)
    revenue = c.fetchall()
#-----------TABLE WITH NUM OF EMPLOYEES------------------------------
    sql ="""SELECT TAB.travel_agency_branch_id, count(1)
            FROM travel_agency_branch TAB, employees EMP
            WHERE TAB.travel_agency_branch_id=EMP.travel_agency_branch_travel_agency_branch_id
            GROUP BY TAB.travel_agency_branch_id
            ORDER BY TAB.travel_agency_branch_id"""
    c.execute(sql)
    employees = c.fetchall()
#-----------TABLE WITH TOTAL SALARIES------------------------------
    sql ="""SELECT TAB.travel_agency_branch_id, sum(EMP.salary)
            FROM employees EMP, travel_agency_branch TAB
            WHERE EMP.travel_agency_branch_travel_agency_branch_id=TAB.travel_agency_branch_id
            GROUP BY TAB.travel_agency_branch_id
            ORDER BY TAB.travel_agency_branch_id"""
    c.execute(sql)
    salaries = c.fetchall()
#-----------------------------------------
        
# Extracting the total_income column
    revcolumn=[tupleobj[1] for tupleobj in revenue]

# Extracting the total_num_of_employees column
    empcolumn=[tupleobj[1] for tupleobj in employees]

# Extracting the total_salary column
    salcolumn=[tupleobj[1] for tupleobj in salaries]
        
        
# Combining the income column to the tuples with the branch_id and the total_num_of_reservations
    res = [sub + (val, ) for sub,val in zip(num_res,revcolumn)]

# Combining the num_of_employees column to the previous one
    res = [sub + (val, ) for sub,val in zip(res,empcolumn)]

# Combining the salary column to the previous one
    res = [sub + (val, ) for sub,val in zip(res,salcolumn)]
        
        
    firstrow = [("travel_agency_branch_id", "total_num_reservations", "total_income", "total_num_employees", "total_salary")]
        
    if x=="ASC" :
# Sorting in ascending 
        res = sorted(res, key=lambda x: x[2], reverse=False)
        return (firstrow + res)
    elif x=="DESC" :
# Sorting in descending
        res = sorted(res, key=lambda x: x[2], reverse=True)
        return (firstrow + res)
    else :
        return ( firstrow + res)

def bestClient(x):

    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    sql ="""SELECT distinct TRA.traveler_id, TA.name
            FROM traveler TRA, reservation RES, guided_tour GT, tourist_attraction TA
            WHERE TRA.traveler_id=RES.Customer_id
        	and RES.offer_trip_package_id=GT.trip_package_id
            and GT.tourist_attraction_id=TA.tourist_attraction_id
            ORDER BY TRA.traveler_id"""
    
    cur.execute(sql)
    attractions = cur.fetchall()
    
    sql ="""SELECT distinct TRA.traveler_id,  sum(offer.cost)
            FROM traveler TRA, reservation RES, offer
            WHERE TRA.traveler_id=RES.Customer_id
        	and RES.offer_id=offer.offer_id
            GROUP BY TRA.traveler_id, TRA.name, TRA.surname
            ORDER BY TRA.traveler_id"""
    
    cur.execute(sql)
    revenue = cur.fetchall()
   
    sql ="""SELECT distinct TRA.traveler_id, TRA.name, TRA.surname, count(distinct DES.country), count(distinct DES.name)
            FROM traveler TRA, reservation RES, trip_package_has_destination TPD, destination DES, offer
            WHERE TRA.traveler_id=RES.Customer_id
        	and RES.offer_trip_package_id=TPD.trip_package_trip_package_id
            and TPD.destination_destination_id=DES.destination_id
            and RES.offer_id=offer.offer_id
            GROUP BY TRA.traveler_id
            ORDER BY TRA.traveler_id"""
        
    cur.execute(sql)
    visits = cur.fetchall()
    
    # Combining the Attractions each of the travelers has gone to
    merged_dict = {}
    for tup in attractions:
        key = tup[0]
        if key in merged_dict:
            merged_dict[key] += ', ' + tup[1]
        else:
            merged_dict[key] = tup[1]

    attractions = [(key, value) for key, value in merged_dict.items()]

 
    res=[]
    i=0
    for index in range(0,len(visits)):
        if(i<len(attractions)):
            new_tup1=attractions[i]
            t1=int(new_tup1[0])
        new_tup2=visits[index]
        t2=int(new_tup2[0])
        if(t1==t2):
            new_tup=(new_tup2[0],new_tup2[1],new_tup2[2],new_tup2[3],new_tup2[4], new_tup1[1])
            res.append(new_tup)
            i=i+1
        else:
            new_tup=(new_tup2[0],new_tup2[1],new_tup2[2],new_tup2[3],new_tup2[4],'')
            res.append(new_tup)

    
    
    result=[]
    i=0
    for index in range(0,len(res)):
        if(i<len(revenue)):
            new_tup1=revenue[i]
            t1=int(new_tup1[0])
        new_tup2=res[index]
        t2=int(new_tup2[0])
        if(t1==t2):
            new_tup=(new_tup2[0],new_tup2[1],new_tup2[2],new_tup2[3],new_tup2[4],new_tup2[5],new_tup1[0],new_tup1[1])
            result.append(new_tup)
            i+=1
        else:
            new_tup=(new_tup2[0],new_tup2[1],new_tup2[2],new_tup2[3],new_tup2[4],new_tup2[5],new_tup1[0],0.0)
            result.append(new_tup)
            i+=2
    
#creating a list of tuples that contains only the customers with the highest revenue(checking if there are multiple)
#   and their requested data
    max_value = max(row[7] for row in result)
    highest_earners = [(row[1:6]) for row in result if row[7] == max_value]
   
    return[("first_name", "last_name", "total_countries_visited", "total_cities_visited", "list_of_attractions")]+highest_earners
    

def giveAway(N):
    con=connection()
    cur=con.cursor()
    return [("string"),]
    

