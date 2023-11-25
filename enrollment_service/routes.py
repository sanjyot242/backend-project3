import contextlib
import sqlite3

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Header, status
import redis

from enrollment_service.database.schemas import Class
from enrollment_service.redis_query import get_waitlist_count, increment_wailist_count , decrement_wailist_count

router = APIRouter()
dropped = []

FREEZE = False
MAX_WAITLIST = 3
database = "enrollment_service/database/database.db"
dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')
redis_client = redis.StrictRedis(host='localhost', port=6379, db =0, decode_responses=True)

# Connect to the database
def get_db():
    with contextlib.closing(sqlite3.connect(database, check_same_thread=False)) as db:
        db.row_factory = sqlite3.Row
        yield db


# Called when a student is dropped from a class / waiting list
# and the enrollment place must be reordered
def reorder_placement(cur, total_enrolled, placement, class_id):
    counter = 1
    while counter <= total_enrolled:
        if counter > placement:
            cur.execute("""UPDATE enrollment SET placement = placement - 1 
                WHERE class_id = ? AND placement = ?""", (class_id,counter))
        counter += 1
    cur.execute("""UPDATE class SET current_enroll = current_enroll - 1
                WHERE id = ?""",(class_id,))


#==========================================students==================================================

#gets available classes for a student
@router.get("/students/{student_id}/classes", tags=['Student']) 
def get_available_classes(student_id: int):
    '''cursor = db.cursor()
    # Fetch student data from db
    cursor.execute(
        """
        SELECT * FROM student
        WHERE id = ?
        """, (student_id,)
    )
    student_data = cursor.fetchone()
    #Check if exist
    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")
    
    # Execute the SQL query to retrieve available classes
    # If max waitlist, don't show full classes with open waitlists
    if student_data['waitlist_count'] >= MAX_WAITLIST:
        cursor.execute("""
            SELECT class.id, class.name, class.course_code, class.section_number, class.current_enroll, class.max_enroll,
                department.id AS department_id, department.name AS department_name,
                instructor.id AS instructor_id, instructor.name AS instructor_name
            FROM class
            INNER JOIN department ON class.department_id = department.id
            INNER JOIN instructor ON class.instructor_id = instructor.id
            WHERE class.current_enroll < class.max_enroll   
        """)
    # Else show all open classes or full classes with open waitlists
    else:
        cursor.execute("""
            SELECT class.id, class.name, class.course_code, class.section_number, class.current_enroll, class.max_enroll,
                department.id AS department_id, department.name AS department_name,
                instructor.id AS instructor_id, instructor.name AS instructor_name
            FROM class
            INNER JOIN department ON class.department_id = department.id
            INNER JOIN instructor ON class.instructor_id = instructor.id
            WHERE class.current_enroll < class.max_enroll + 15   
        """)'''
    student_table = dynamodb.Table('student')
    class_table = dynamodb.Table('class')
    department_table = dynamodb.Table('department')
    instructor_table = dynamodb.Table('instructor')

    #get student data from student table 
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    
    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")
    
    # Initialize classes list
    classes = []
    '''if student_data['waitlist_count'] >= MAX_WAITLIST:
        print(student_data['waitlist_count'])
        # Logic for classes with current_enroll < max_enroll
        class_response = class_table.scan(FilterExpression='current_enroll < max_enroll')
        classes = class_response.get('Items')
    else:
        print("Inside else")
        class_response = class_table.scan()
        all_classes = class_response.get('Items')

        # Filtering classes based on the condition: current_enroll < max_enroll + 15
        classes = [c for c in all_classes if c['current_enroll'] < (c['max_enroll'] + 15)]'''
    student_waitlist_count = get_waitlist_count(student_id=student_id, redis_client=redis_client)
    #check waitlist count for the student 
    if student_waitlist_count >= MAX_WAITLIST:
        class_response = class_table.query(
            IndexName='AvailableSlotsIndex',
            KeyConditionExpression=Key('constantGSI').eq("ALL") & Key('available_slot').gt(0)
    )
    else:
        
        class_response = class_table.query(
            IndexName='AvailableSlotsIndex',
            KeyConditionExpression=Key('constantGSI').eq("ALL") & Key('available_slot').gt(-15)
        )

    classes = class_response.get('Items')

    #joining the data to get desired response 
    classes_with_details = []
    for c in classes:
        department = department_table.get_item(Key={'id': c['department_id']}).get('Item')
        instructor = instructor_table.get_item(Key={'id': c['instructor_id']}).get('Item')
        class_info = {
            'id': c['id'],
            'name': c['name'],
            'course_code': c['course_code'],
            'section_number': c['section_number'],
            'current_enroll': c['current_enroll'],
            'max_enroll': c['max_enroll'],
            'department_id': department['id'],
            'department_name': department['name'],
            'instructor_id': instructor['id'],
            'instructor_name': instructor['name']
        }
        classes_with_details.append(class_info)

    return {"Classes": classes_with_details}

#gets currently enrolled classes for a student
@router.get("/students/{student_id}/enrolled", tags=['Student'])
def view_enrolled_classes(student_id: int, db: sqlite3.Connection = Depends(get_db)):
    '''cursor = db.cursor()
    
    # Check if the student exists in the database
    cursor.execute("SELECT * FROM student WHERE id = ?", (student_id,))
    student_data = cursor.fetchone()

    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    # Check if the student is enrolled in any classes
    cursor.execute("""
        SELECT class.id, department.name AS department_name, class.course_code, class.section_number, class.name AS class_name, class.current_enroll
            FROM enrollment
            JOIN class ON enrollment.class_id = class.id
            JOIN student ON enrollment.student_id = student.id
            JOIN department ON class.department_id = department.id
            WHERE student.id = ? AND class.current_enroll < class.max_enroll
        """, (student_id,))
    student_data = cursor.fetchall()'''
    student_table = dynamodb.Table('student')
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    enrollment_table = dynamodb.Table('enrollment')
    department_table = dynamodb.Table('department')
    class_table = dynamodb.Table('class')

    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    response = enrollment_table.query(
    KeyConditionExpression=Key('student_id').eq(student_id)  
    )

    #print(response)



#creating list to store class details 
    enrolled_classes = []

    # Loop through the class_id in response 
    for item in response['Items']:
        class_id = item['class_id']
        #get deatails of particiular classes in class_response
        class_response = class_table.get_item(
            Key={'id': class_id}  
        )
        
        if 'Item' in class_response:
            enrolled_class = class_response['Item']
            #logic to only show classes which the student is enrolled in 
            if enrolled_class.get('current_enroll') < enrolled_class.get('max_enroll'):
                department = department_table.get_item(Key={'id': enrolled_class['department_id']}).get('Item')
                enrolled_classes.append({
                    "id": enrolled_class.get('id'),
                    "department_name": department.get('name'),
                    "course_code": enrolled_class.get('course_code'),
                    "section_number": enrolled_class.get('section_number'),
                    "class_name": enrolled_class.get('name'),
                    "current_enroll": enrolled_class.get('current_enroll')
                })

    # Construct the final response
    final_response = {"Enrolled": enrolled_classes}





    if not final_response:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not enrolled in any classes")
    
    return  final_response


# Enrolls a student into an available class,
# or will automatically put the student on an open waitlist for a full class
@router.post("/students/{student_id}/classes/{class_id}/enroll", tags=['Student'])
def enroll_student_in_class(student_id: int, class_id: int, db: sqlite3.Connection = Depends(get_db)):
    student_table = dynamodb.Table('student')
    class_table = dynamodb.Table('class')
    enrollment_table = dynamodb.Table('enrollment')
    #get student data from student table 
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    
    class_response = class_table.get_item(Key={'id': class_id} )
    class_data = class_response.get('Item')
    
    print(student_data)
    print(class_data)
    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")

    #check if student is already enrolled in the class
    enrollment_response = enrollment_table.query(
        KeyConditionExpression = boto3.dynamodb.conditions.Key('student_id').eq(student_id),
        ProjectionExpression='class_id'
        )
    enrollment_data = enrollment_response["Items"]

    
    exists = any(item['class_id'] == class_id for item in enrollment_data)
    
    if(exists):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is already enrolled or waitlisted in this class")
    
    
    #if class is not full add to enrollment
    if class_data['current_enroll'] >= class_data['max_enroll']:
        if not FREEZE:
            #add to waitlist if place exists in waitlist 
            waitlist_count = get_waitlist_count(student_id=student_data['id'], redis_client=redis_client)
            if waitlist_count < MAX_WAITLIST:
                increment_wailist_count(student_id=student_data['id'], redis_client=redis_client)
                return {"message": "Student added to the waitlist"}
            else:
                return {"message": "Unable to add student to waitlist due to already having max number of waitlists"}
        else:
            return {"message": "Unable to add student to waitlist due to administrative freeze"}
    
    
    

    #increase enrollemt number in class db 
    response = class_table.update_item(
    Key={
        'id': class_id
    },
    UpdateExpression="SET current_enroll = current_enroll + :inc",
    ExpressionAttributeValues={
        ':inc': 1
    },
    ReturnValues="UPDATED_NEW"
    )
    print(response)
    current_enrolled = response['Attributes']['current_enroll']
    #add student to enrolled class 
    data_item = {
    'student_id': student_id,  # Assuming 'student_id' is the primary key
    'placement': current_enrolled,
    'class_id': class_id
    }
    enrollment_table.put_item(Item=data_item)
    
    #fecth updated class and display details 
    class_response = class_table.get_item(Key={'id': class_id} )
    class_data = class_response.get('Item')

            

    return class_data
        
        
    '''cursor = db.cursor()
    # Check if the student exists in the database
    cursor.execute("SELECT * FROM student WHERE id = ?", (student_id,))
    student_data = cursor.fetchone()

    # Check if the class exists in the database
    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")

    # Check if student is already enrolled in the class
    cursor.execute("""SELECT * FROM enrollment
                    JOIN class ON enrollment.class_id = class.id
                    WHERE class_id = ? AND student_id = ?
                    AND class.current_enroll <= class.max_enroll
                    """, (class_id, student_id))
    existing_enrollment = cursor.fetchone()

    if existing_enrollment:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is already enrolled in this class")
    
    # Check if the class is full, add student to waitlist if no
    # freeze is in place
    if class_data['current_enroll'] >= class_data['max_enroll']:
        if not FREEZE:
            # if student_data['waitlist_count'] < MAX_WAITLIST:
            #     cursor.execute("""UPDATE student 
            #                     SET waitlist_count = waitlist_count + 1
            #                     WHERE id = ?""",(student_id,))
            #     return {"message": "Student added to the waitlist"}
            # else:
            #     return {"message": "Unable to add student to waitlist due to already having max number of waitlists"}
            waitlist_count = get_waitlist_count(student_id=student_data['id'], redis_client=redis_client)
            if waitlist_count < MAX_WAITLIST:
                increment_wailist_count(student_id=student_data['id'], redis_client=redis_client)
                return {"message": "Student added to the waitlist"}
            else:
                return {"message": "Unable to add student to waitlist due to already having max number of waitlists"}
        else:
            return {"message": "Unable to add student to waitlist due to administrative freeze"}
    
    # Increment enrollment number in the database
    new_enrollment = class_data['current_enroll'] + 1
    cursor.execute("UPDATE class SET current_enroll = ? WHERE id = ?", (new_enrollment, class_id))

    # Add student to enrolled class in the database
    cursor.execute("INSERT INTO enrollment (placement, class_id, student_id) VALUES (?, ?, ?)", (new_enrollment, class_id, student_id))
    
    # Remove student from dropped table if valid
    cursor.execute("""SELECT * FROM dropped 
                    WHERE class_id = ? AND student_id = ?
                    """, (class_id, student_id))
    dropped_data = cursor.fetchone()
    if dropped_data:
        cursor.execute("""DELETE FROM dropped 
                    WHERE class_id = ? AND student_id = ?
                    """, (class_id, student_id))
    
    db.commit()

    # Fetch the updated class data from the database
    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    updated_class_data = cursor.fetchone()
   
    return updated_class_data'''


# Have a student drop a class they're enrolled in
@router.delete("/students/classes/{class_id}", tags=['Students drop their own classes'])
def drop_student_from_class(class_id: int, student_id: int = Header(None, alias="x-cwid"), db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    # check if exist
    cursor.execute("SELECT * FROM student WHERE id = ?", (student_id,))
    student_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")

    #check enrollment
    cursor.execute("SELECT * FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    enrollment_data = cursor.fetchone()

    cursor.execute("""SELECT * FROM enrollment
                    JOIN class ON enrollment.class_id = class.id
                    WHERE enrollment.student_id = ?
                    AND enrollment.placement > class.max_enroll""", (student_id,))
    waitlist_data = cursor.fetchone()
    
    if not enrollment_data and not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not enrolled in the class")

    # remove student from class
    cursor.execute("DELETE FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    reorder_placement(cursor, class_data['current_enroll'], enrollment_data['placement'], class_id)

    # Update dropped table
    cursor.execute(""" INSERT INTO dropped (class_id, student_id)
                    VALUES (?, ?)""",(class_id, student_id))
    db.commit()
    
    # Fetch data to return
    cursor.execute("""SELECT * FROM dropped
                    WHERE class_id = ? and student_id = ?""",(class_id, student_id))
    dropped_data = cursor.fetchone() 
    return dropped_data


#==========================================wait list========================================== 


# Get all classes with waiting lists
@router.get("/waitlist/classes", tags=['Waitlist'])
def view_all_class_waitlists(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()  

    # fetch all relevant waitlist information for student
    cursor.execute("""
        SELECT class.id AS class_id, department.id AS department_id, class.course_code, 
        class.section_number, class.name AS class_name, instructor.id AS instructor_id,
        class.current_enroll - class.max_enroll AS waitlist_total
        FROM class
        JOIN department ON class.department_id = department.id
        JOIN instructor ON class.instructor_id = instructor.id
        WHERE class.current_enroll > class.max_enroll
        """
    )
    waitlist_data = cursor.fetchall()
    # Check if exist
    if not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No classes have waitlists")

    return {"Waitlists": waitlist_data}


# Get all waiting lists for a student
@router.get("/waitlist/students/{student_id}", tags=['Waitlist'])
def view_waiting_list(student_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    # Retrieve waitlist entries for the specified student from the database
    # cursor.execute("SELECT waitlist_count FROM student WHERE id = ? AND waitlist_count > 0", (student_id,))
    # waitlist_data = cursor.fetchall()
    waitlist_data = get_waitlist_count(student_id=student_id, redis_client=redis_client)

    # Check if exist
    if waitlist_data == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not on a waitlist")  

    # fetch all relevant waitlist information for student
    cursor.execute("""
        SELECT class.id, department.name AS department_name, class.course_code, 
        class.section_number, class.name AS class_name, instructor.name AS instructor_name,
        enrollment.placement - class.max_enroll AS waitlist_placement
        FROM enrollment
        JOIN class ON enrollment.class_id = class.id
        JOIN student ON enrollment.student_id = student.id
        JOIN department ON class.department_id = department.id
        JOIN instructor ON class.instructor_id = instructor.id
        WHERE student.id = ? AND class.current_enroll > class.max_enroll
        """, (student_id,)
    )
    waitlist_data = cursor.fetchall()

    return {"Waitlists": waitlist_data}


# remove a student from a waiting list
@router.put("/waitlist/students/{student_id}/classes/{class_id}/drop", tags=['Waitlist'])
def remove_from_waitlist(student_id: int, class_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    
    # check if exist
    cursor.execute("SELECT * FROM student WHERE id = ?", (student_id,))
    student_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")
    
    # cursor.execute("SELECT * FROM student WHERE id = ? AND waitlist_count > 0", (student_id,))
    # student_data = cursor.fetchone()
    student_data = get_waitlist_count(student_id=student_id, redis_client=redis_client)

    if student_data == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not on the waitlist")

    cursor.execute("""SELECT enrollment.placement, class.current_enroll
                    FROM enrollment 
                    JOIN class ON enrollment.class_id = class.id
                    WHERE student_id = ? AND class_id = ?
                    AND enrollment.placement > class.max_enroll
                    """, (student_id, class_id))
    waitlist_entry = cursor.fetchone()

    if waitlist_entry is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student is not on the waiting list for this class")

    # Delete student from waitlist enrollment
    cursor.execute("DELETE FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    # cursor.execute("""UPDATE student SET waitlist_count = waitlist_count - 1
    #                 WHERE id = ?""", (student_id,))
    decrement_wailist_count(student_id = student_id, redis_client=redis_client)
    
    # Reorder enrollment placements
    reorder_placement(cursor, waitlist_entry['current_enroll'], waitlist_entry['placement'], class_id)
    db.commit()

    return {"message": "Student removed from the waiting list"}

# Get a list of students on a waitlist for a particular class that
# a specific instructor teaches
@router.get("/waitlist/instructors/{instructor_id}/classes/{class_id}",tags=['Waitlist'])
def view_current_waitlist(instructor_id: int, class_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

   # check if exist
    cursor.execute("SELECT * FROM instructor WHERE id = ?", (instructor_id,))
    instructor_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor or Class not found")  

    # fetch all relevant waitlist information for instructor
    cursor.execute("""
        SELECT class.id AS class_id, department.name AS department_name, class.course_code, 
        class.section_number, class.name AS class_name, enrollment.student_id AS student_id, 
        enrollment.placement - class.max_enroll AS waitlist_placement
        FROM enrollment
        JOIN class ON enrollment.class_id = class.id
        JOIN department ON class.department_id = department.id
        JOIN instructor ON class.instructor_id = instructor.id
        WHERE instructor.id = ? AND class.current_enroll > class.max_enroll
        AND enrollment.placement > class.max_enroll
        """, (instructor_id,)
    )
    waitlist_data = cursor.fetchall()

    #Check if exist
    if not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class does not have a waitlist")

    return {"Waitlist": waitlist_data}


#==========================================Instructor==================================================


#view current enrollment for class
@router.get("/instructors/{instructor_id}/classes/{class_id}/enrollment", tags=['Instructor'])
def get_instructor_enrollment(instructor_id: int, class_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    #check if exist
    cursor.execute("SELECT * FROM instructor WHERE id = ?", (instructor_id,))
    instructor_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or class not found")

    cursor.execute("SELECT * FROM class WHERE id = ? AND instructor_id = ?", (class_id,instructor_id))
    assigned_data = cursor.fetchone()

    if not assigned_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to Class")

    #Fetch relavent data for instructor
    cursor.execute("""SELECT class.id AS class_id, class.name AS class_name, department.name AS department_name,
                    class.course_code, class.section_number, class.max_enroll,
                    student.id AS student_id, student.name AS student_name, enrollment.placement
                    FROM enrollment 
                    JOIN class ON enrollment.class_id = class.id
                    JOIN student ON enrollment.student_id = student.id
                    JOIN department ON class.department_id = department.id
                    JOIN instructor ON class.instructor_id = instructor.id
                    WHERE class.instructor_id = ? AND enrollment.class_id = ? 
                    AND enrollment.placement <= class.max_enroll""", (instructor_id, class_id))
    enrolled_data = cursor.fetchall()

    return {"Enrolled" : enrolled_data}


#view students who have dropped the class
@router.get("/instructors/{instructor_id}/classes/{class_id}/drop", tags=['Instructor'])
def get_instructor_dropped(instructor_id: int, class_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    #Check if exist
    cursor.execute("SELECT * FROM instructor WHERE id = ?", (instructor_id,))
    instructor_data = cursor.fetchone()
    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or class not found")

    cursor.execute("SELECT * FROM class WHERE id = ? AND instructor_id = ?", (class_id,instructor_id))
    assigned_data = cursor.fetchone()

    if not assigned_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to Class")
    
    cursor.execute("""SELECT student.id AS student_id, student.name AS student_name
                        FROM dropped 
                        JOIN student ON dropped.student_id = student.id
                        WHERE dropped.class_id = ?""", (class_id,))
    dropped_data = cursor.fetchall()
    
    return dropped_data


#Instructor administratively drop students
@router.post("/instructors/{instructor_id}/classes/{class_id}/students/{student_id}/drop", tags=['Instructor'])
def instructor_drop_class(instructor_id: int, class_id: int, student_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    #Check if exist
    cursor.execute("SELECT * FROM instructor WHERE id = ?", (instructor_id,))
    instructor_data = cursor.fetchone()
    
    cursor.execute("SELECT * FROM student WHERE id = ?", (student_id,))
    student_data = cursor.fetchone()

    if not instructor_data or not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or student not found")

    cursor.execute("SELECT * FROM class WHERE id = ? AND instructor_id = ?", (class_id, instructor_id))
    target_class_data = cursor.fetchone()

    if not target_class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found or instructor is not teaching this class")

    cursor.execute("""SELECT * FROM enrollment
                        WHERE class_id = ? AND student_id = ?
                    """,(class_id, student_id))
    enroll_data = cursor.fetchone()
    if not enroll_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not enrolled in class")
    
    # remove student from class
    cursor.execute("DELETE FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    reorder_placement(cursor, target_class_data['current_enroll'], enroll_data['placement'], class_id)

    db.commit()

    #Fetch relavent data for instructor
    cursor.execute("""SELECT class.id AS class_id, class.name AS class_name, department.name AS department_name,
                    class.course_code, class.section_number, class.max_enroll,
                    student.id AS student_id, student.name AS student_name, enrollment.placement
                    FROM enrollment 
                    JOIN class ON enrollment.class_id = class.id
                    JOIN student ON enrollment.student_id = student.id
                    JOIN department ON class.department_id = department.id
                    JOIN instructor ON class.instructor_id = instructor.id
                    WHERE class.instructor_id = ? AND enrollment.class_id = ? 
                    AND enrollment.placement <= class.max_enroll""", (instructor_id, class_id))
    enrolled_data = cursor.fetchall()
    

    return {"Enrollment" : enrolled_data}


#==========================================registrar==================================================


# Create a new class
@router.post("/registrar/classes/", response_model=Class, tags=['Registrar'])
def create_class(class_data: Class, db: sqlite3.Connection = Depends(get_db)):
    try:
        db.execute(
            """
            INSERT INTO class (id, name, course_code, section_number, current_enroll, max_enroll, department_id, instructor_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                class_data.id,
                class_data.name,
                class_data.course_code,
                class_data.section_number,
                class_data.current_enroll,
                class_data.max_enroll,
                class_data.department_id,
                class_data.instructor_id
            )
        )
        db.commit()
        return class_data
    except sqlite3.IntegrityError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"type": type(e).__name__, "msg": str(e)}
        )

# Remove a class
@router.delete("/registrar/classes/{class_id}", tags=['Registrar'])
def remove_class(class_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    # Check if the class exists in the database
    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    target_class_data = cursor.fetchone()

    if not target_class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found")

    # Delete the class from the database
    cursor.execute("DELETE FROM class WHERE id = ?", (class_id,))
    db.commit()

    return {"message": "Class removed successfully"}


# Change the assigned instructor for a class
@router.put("/registrar/classes/{class_id}/instructors/{instructor_id}", tags=['Registrar'])
def change_instructor(class_id: int, instructor_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    target_class_data = cursor.fetchone()

    if not target_class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found")

    cursor.execute("SELECT * FROM instructor WHERE id = ?", (instructor_id,))
    instructor_data = cursor.fetchone()

    if not instructor_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor not found")

    cursor.execute("UPDATE class SET instructor_id = ? WHERE id = ?", (instructor_id, class_id))
    db.commit()

    return {"message": "Instructor changed successfully"}


# Freeze enrollment for classes
@router.put("/registrar/automatic-enrollment/freeze", tags=['Registrar'])
def freeze_automatic_enrollment():
    global FREEZE
    if FREEZE:
        FREEZE = False
        return {"message": "Automatic enrollment unfrozen successfully"}
    else:
        FREEZE = True
        return {"message": "Automatic enrollment frozen successfully"}

######Helpers#####
def update_class_availability(dynamodb_client, class_id, max_enroll, current_enroll):
    class_table = dynamodb_client.Table('class')
    available_slots = max_enroll - current_enroll

    class_table.update_item(
        Key={'id': class_id},
        UpdateExpression="SET availableSlots = :val",
        ExpressionAttributeValues={':val': available_slots}
    )