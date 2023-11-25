import contextlib
import sqlite3
import boto3
import redis
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from fastapi import Depends, HTTPException, APIRouter, Header, status
from enrollment_service.database.schemas import Class
from enrollment_service.redis_query import get_waitlist_count, increment_wailist_count, decrement_wailist_count

router = APIRouter()
dropped = []

FREEZE = False
MAX_WAITLIST = 3
database = "enrollment_service/database/database.db"
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:5500')


# Connect to the database
def get_db():
    with contextlib.closing(sqlite3.connect(database, check_same_thread=False)) as db:
        db.row_factory = sqlite3.Row
        yield db


# Called when a student is dropped from a class / waiting list
# and the enrollment place must be reordered
def reorder_placement(cur, total_enrolled, placement, class_id):

    # TODO: Rewrite this method using dynamoDB and update all calls to it.
    counter = 1
    while counter <= total_enrolled:
        if counter > placement:
            cur.execute("""UPDATE enrollment SET placement = placement - 1 
                WHERE class_id = ? AND placement = ?""", (class_id,counter))
        counter += 1
    cur.execute("""UPDATE class SET current_enroll = current_enroll - 1
                WHERE id = ?""",(class_id,))


# ==========================================students==================================================

# gets available classes for a student
@router.get("/students/{student_id}/classes", tags=['Student']) 
def get_available_classes(student_id: int):
    student_table = dynamodb.Table('student')
    class_table = dynamodb.Table('class')
    department_table = dynamodb.Table('department')
    instructor_table = dynamodb.Table('instructor')

    # get student data from student table
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
        # Using query with GSI - classes that can have wait listed students
        class_response = class_table.query(
            IndexName='AvailableSlotsIndex',
            KeyConditionExpression=Key('constantGSI').eq("ALL") & Key('available_slot').gt(-15)
        )

    classes = class_response.get('Items')

    # joining the data
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


# gets currently enrolled classes for a student
@router.get("/students/{student_id}/enrolled", tags=['Student'])
def view_enrolled_classes(student_id: int):
    student_table = dynamodb.Table('student')
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    enrollment_table = dynamodb.Table('enrollment')
    department_table = dynamodb.Table('department')
    class_table = dynamodb.Table('class')

    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    # Query DynamoDB
    response = enrollment_table.query(
        KeyConditionExpression=Key('student_id').eq(student_id)
    )

    # creating list to store class details
    enrolled_classes = []

    # Loop through the class_id in response 
    for item in response['Items']:
        class_id = item['class_id']
        # get details of particular classes in class_response
        class_response = class_table.get_item(
            Key={'id': class_id}  
        )
        
        if 'Item' in class_response:
            enrolled_class = class_response['Item']
            # logic to only show classes which the student is enrolled in
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

    return final_response


# Enrolls a student into an available class,
# or will automatically put the student on an open waitlist for a full class
@router.post("/students/{student_id}/classes/{class_id}/enroll", tags=['Student'])
def enroll_student_in_class(student_id: int, class_id: int):
    student_table = dynamodb.Table('student')
    class_table = dynamodb.Table('class')
    enrollment_table = dynamodb.Table('enrollment')
    # get student data from student table
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    
    class_response = class_table.get_item(Key={'id': class_id})
    class_data = class_response.get('Item')
    
    print(student_data)
    print(class_data)
    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")

    # check if student is already enrolled in the class
    enrollment_response = enrollment_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('student_id').eq(student_id),
        ProjectionExpression='class_id'
    )
    enrollment_data = enrollment_response["Items"]

    exists = any(item['class_id'] == class_id for item in enrollment_data)
    
    if exists:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is already enrolled or wait listed"
                                                                            " in this class")

    # if class is not full add to enrollment
    if class_data['current_enroll'] >= class_data['max_enroll']:
        if not FREEZE:
            # add to waitlist if place exists in waitlist
            waitlist_count = get_waitlist_count(student_id=student_data['id'], redis_client=redis_client)
            if waitlist_count < MAX_WAITLIST:
                increment_wailist_count(student_id=student_data['id'], redis_client=redis_client)
                return {"message": "Student added to the waitlist"}
            else:
                return {"message": "Unable to add student to waitlist due to already having max number of wait-lists"}
        else:
            return {"message": "Unable to add student to waitlist due to administrative freeze"}

    # increase enrollment number in class db
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
    # add student to enrolled class
    data_item = {
        'student_id': student_id,  # Assuming 'student_id' is the primary key
        'placement': current_enrolled,
        'class_id': class_id
    }
    enrollment_table.put_item(Item=data_item)
    
    # fetch updated class and display details
    class_response = class_table.get_item(Key={'id': class_id} )
    class_data = class_response.get('Item')

    return class_data


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
# TODO: Update to use redis
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No classes have wait-lists")

    return {"Waitlists": waitlist_data}


# Get all waiting lists for a student
# TODO: Update to use redis
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
# TODO: Update to use redis
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
    decrement_wailist_count(student_id=student_id, redis_client=redis_client)
    
    # Reorder enrollment placements
    reorder_placement(cursor, waitlist_entry['current_enroll'], waitlist_entry['placement'], class_id)
    db.commit()

    return {"message": "Student removed from the waiting list"}


# Get a list of students on a waitlist for a particular class that
# a specific instructor teaches
# TODO: Update to use redis
@router.get("/waitlist/instructors/{instructor_id}/classes/{class_id}", tags=['Waitlist'])
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


# ==========================================Instructor==================================================


# View list of current students enrolled for class
@router.get("/instructors/{instructor_id}/classes/{class_id}/enrollment", tags=['Instructor'])
def get_instructor_enrollment(instructor_id: int, class_id: int):
    class_table = dynamodb.Table('class')
    student_table = dynamodb.Table('student')
    enrollment_table = dynamodb.Table('enrollment')

    class_response = class_table.get_item(Key={'id': class_id})
    class_data = class_response.get('Item')

    check_instructor_or_class_exist(instructor_id, class_id)

    # Query for enrollment data by class_id
    enrollment_data = enrollment_table.query(
        IndexName='ClassIndex',
        KeyConditionExpression=Key('class_id').eq(class_id)
    )

    student_data = []

    for student in enrollment_data['Items']:
        student_name = student_table.get_item(Key={'id': student['student_id']}).get('Item').get('name')
        student_data.append({'name': student_name, 'placement': student['placement']})

    class_data['students_enrolled'] = student_data

    return {"Enrolled": class_data}


# View list of students who have dropped the class
@router.get("/instructors/{instructor_id}/classes/{class_id}/drop", tags=['Instructor'])
def get_instructor_dropped(instructor_id: int, class_id: int):
    student_table = dynamodb.Table('student')
    dropped_table = dynamodb.Table('dropped')

    dropped_data = dropped_table.query(
        KeyConditionExpression=Key('class_id').eq(class_id)
    )

    check_instructor_or_class_exist(instructor_id, class_id)

    result = []
    for student in dropped_data['Items']:
        student_ids = student_table.get_item(Key={'id': student.get('student_id')})
        result.append(student_ids['Item']['name'])

    return {'dropped_students': result}


# Instructor administratively drops student
@router.post("/instructors/{instructor_id}/classes/{class_id}/students/{student_id}/drop", tags=['Instructor'])
def instructor_drop_class(instructor_id: int, class_id: int, student_id: int):
    enrollment_table = dynamodb.Table('enrollment')

    check_student_exists(student_id)
    check_instructor_or_class_exist(instructor_id, class_id)

    # Check if student is in the class
    enrollment_response = enrollment_table.get_item(Key={'student_id': student_id, 'class_id': class_id})
    enrollment_data = enrollment_response.get('Item')

    if not enrollment_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not enrolled in selected class")

    # Drop student from class
    enrollment_table.delete_item(Key={'student_id': student_id, 'class_id': class_id})
    # TODO: Will need to call reorder method after student is removed from class to change enrollment order number

    # We will return the new list of students enrolled in the class
    return get_instructor_enrollment(instructor_id=instructor_id, class_id=class_id)


# ==========================================registrar==================================================


# Create a new class
@router.post("/registrar/classes/", tags=['Registrar'])
def create_class(class_data: Class):
    class_table = dynamodb.Table('class')
    class_response = class_table.get_item(Key={'id': class_data.id}).get('Item')
    available_slot = class_data.max_enroll - class_data.current_enroll

    # Check if class id exists
    if class_response:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Class id already exists")
    else:
        class_table.put_item(
            Item={
                'id': class_data.id,
                'name': class_data.name,
                'course_code': class_data.course_code,
                'section_number': class_data.section_number,
                'current_enroll': class_data.current_enroll,
                'max_enroll': class_data.max_enroll,
                'department_id': class_data.department_id,
                'instructor_id': class_data.instructor_id,
                'available_slot': available_slot,
                'constantGSI': "ALL"
            }
        )

    return {"http_status_code": status.HTTP_201_CREATED, "http_body": "class created"}


# Remove a class
@router.delete("/registrar/classes/{class_id}", tags=['Registrar'])
def remove_class(class_id: int):
    class_table = dynamodb.Table('class')
    check_class_exists(class_id)

    class_table.delete_item(Key={'id': class_id})

    return {"http_status_code": status.HTTP_201_CREATED, "http_body": "class deleted"}


# Change the assigned instructor for a class
@router.put("/registrar/classes/{class_id}/instructors/{instructor_id}", tags=['Registrar'])
def change_instructor(class_id: int, instructor_id: int):
    check_class_exists(class_id)
    check_instructor_exists(instructor_id)

    class_table = dynamodb.Table('class')
    class_table.update_item(
        Key={'id': class_id},
        UpdateExpression="SET instructor_id = :i",
        ExpressionAttributeValues={":i": instructor_id}
    )

    return {"http_status_code": status.HTTP_201_CREATED, "http_body": "instructor changed"}


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


# ==========================================helpers==================================================
def update_class_availability(dynamodb_client, class_id, max_enroll, current_enroll):
    class_table = dynamodb_client.Table('class')
    available_slots = max_enroll - current_enroll

    class_table.update_item(
        Key={'id': class_id},
        UpdateExpression="SET availableSlots = :val",
        ExpressionAttributeValues={':val': available_slots}
    )


def check_instructor_or_class_exist(instructor_id: int, class_id: int):
    class_table = dynamodb.Table('class')
    instructor_table = dynamodb.Table('instructor')

    # Make sure instructor and class id exist
    instructor_response = instructor_table.get_item(Key={'id': instructor_id})
    instructor_data = instructor_response.get('Item')

    class_response = class_table.get_item(Key={'id': class_id})
    class_data = class_response.get('Item')

    if not instructor_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor doesnt exist")

    if not class_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class doesnt exist")

    # Make sure the class and instructor match
    if class_data['instructor_id'] != instructor_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor doesn't teach class selected")


def check_class_exists(class_id: int):
    class_table = dynamodb.Table('class')
    class_response = class_table.get_item(Key={'id': class_id}).get('Item')

    if not class_response:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found")


def check_instructor_exists(instructor_id: int):
    instructor_table = dynamodb.Table('instructor')
    instructor_response = instructor_table.get_item(Key={'id': instructor_id})
    instructor_data = instructor_response.get('Item')
    if not instructor_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor doesnt exist")


def check_student_exists(student_id: int):
    student_table = dynamodb.Table('student')
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student doesnt exist")
