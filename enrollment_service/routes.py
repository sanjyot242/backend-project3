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
def drop_student_from_class(class_id: int, student_id: int = Header(None, alias="x-cwid")):
    student_table = dynamodb.Table('student')
    class_table = dynamodb.Table('class')
    enrollment_table = dynamodb.Table('enrollment')
    # get student data from student table
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    
    class_response = class_table.get_item(Key={'id': class_id})
    class_data = class_response.get('Item')
    dropped_table = dynamodb.Table('dropped')
    print(student_data)
    print(class_data)
    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")


     # check if student is already enrolled in the class
    enrollment_response = enrollment_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('student_id').eq(student_id),
    )
    enrollment_data = enrollment_response["Items"]

    waitlist_data = None
    for enrollment in enrollment_data:
        # For each enrollment, get the class details
        class_response = class_table.get_item(
            Key={'id': enrollment['class_id']}
        )
        class_item = class_response.get('Item')

        if class_item and enrollment['placement'] > class_item['max_enroll']:
            #get 
            waitlist_data = {'enrollment': enrollment, 'class': class_item}
            break  

    if not enrollment_data and not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not enrolled in the class")
    
    enrollment_table.delete_item(
        Key={
            'student_id': student_id,
            'class_id': class_id
        }
    )
    reorder_placement_dynamodb(enrollment['placement'],class_id)


    dropped_table.put_item(
        Item={
            'class_id': class_id,
            'student_id': student_id
        }
    )

    response = dropped_table.get_item(
        Key={
            'class_id': class_id,
            'student_id': student_id
        }
    )

    # Extract the item from the response
    dropped_data = response.get('Item', None)

    return dropped_data

#==========================================wait list========================================== 


# Get all classes with waiting lists
@router.get("/waitlist/classes", tags=['Waitlist'])
def view_all_class_waitlists():
    class_table = dynamodb.Table('class')
    response = class_table.query(
            IndexName='AvailableSlotsIndex',
            KeyConditionExpression=Key('constantGSI').eq("ALL") & Key('available_slot').lt(0)
    )
    return {"Waitlists": response}


# Get all waiting lists for a student
@router.get("/waitlist/students/{student_id}", tags=['Waitlist'])
def view_waiting_list(student_id: int, ):
    waitlist_data = get_waitlist_count(student_id=student_id, redis_client=redis_client)

    # Check if exist
    if waitlist_data == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not on a waitlist")  
    student_table = dynamodb.Table('student')
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    enrollment_table = dynamodb.Table('enrollment')
    department_table = dynamodb.Table('department')
    class_table = dynamodb.Table('class')
    # Query DynamoDB
    response = enrollment_table.query(
        KeyConditionExpression=Key('student_id').eq(student_id)
    )
    # creating list to store class details
    waitlist_classes = []

    # Loop through the class_id in response 
    for item in response['Items']:
        class_id = item['class_id']
        # get details of particular classes in class_response
        class_response = class_table.get_item(
            Key={'id': class_id}  
        )
        if 'Item' in class_response:
            waitlist_class = class_response['Item']
            # logic to only show classes which the student is waitlist in
            if waitlist_class.get('current_enroll') > waitlist_class.get('max_enroll'):
                department = department_table.get_item(Key={'id': waitlist_class['department_id']}).get('Item')
                waitlist_classes.append({
                    "id": waitlist_class.get('id'),
                    "department_name": department.get('name'),
                    "course_code": waitlist_class.get('course_code'),
                    "section_number": waitlist_class.get('section_number'),
                    "class_name": waitlist_class.get('name'),
                    "current_enroll": waitlist_class.get('current_enroll')
                })
    # Construct the final response
    final_response = {"Waitlist": waitlist_classes}
    return final_response

# remove a student from a waiting list
@router.put("/waitlist/students/{student_id}/classes/{class_id}/drop", tags=['Waitlist'])
def remove_from_waitlist(student_id: int, class_id: int, ):
    student_table = dynamodb.Table('student')
    student_response = student_table.get_item(Key={'id': student_id})
    student_data = student_response.get('Item')
    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    class_table = dynamodb.Table('class')
    class_response = class_table.get_item(Key={'id': class_id})
    class_data = class_response.get('Item')
    if not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found")
    
    student_data = get_waitlist_count(student_id=student_id, redis_client=redis_client)

    if student_data == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not on the waitlist")
    enrollment_table = dynamodb.Table('enrollment')
    # Check if student is in the class
    enrollment_response = enrollment_table.get_item(Key={'student_id': student_id, 'class_id': class_id})
    enrollment_data = enrollment_response.get('Item')

    if not enrollment_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not enrolled in selected class")
    waitlist_entry = None
    # for item in enrollment_data['Items']:
    class_id = enrollment_data['class_id']
    # get details of particular classes in class_response
    class_response = class_table.get_item(Key={'id': class_id} )
    if 'Item' in class_response:
        waitlist_class = class_response['Item']
        if enrollment_data['placement'] > waitlist_class['max_enroll']:
            waitlist_entry = waitlist_class

    if waitlist_entry is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student is not on the waiting list for this class")

    enrollment_table.delete_item(Key={'student_id': student_id, 'class_id': class_id})
    decrement_wailist_count(student_id=student_id, redis_client=redis_client)
    reorder_placement_dynamodb(enrollment_data['placement'],class_id)
  
    return {"message": "Student removed from the waiting list"}


# Get a list of students on a waitlist for a particular class that
# a specific instructor teaches
@router.get("/waitlist/instructors/{instructor_id}/classes/{class_id}", tags=['Waitlist'])
def view_current_waitlist(instructor_id: int, class_id: int, ):
    class_table = dynamodb.Table('class')
    enrollment_table = dynamodb.Table('enrollment')
    department_table = dynamodb.Table('department')
    check_instructor_or_class_exist(instructor_id, class_id)

    # Query for enrollment data by class_id
    enrollment_data = enrollment_table.query(
        IndexName='ClassIndex',
        KeyConditionExpression=Key('class_id').eq(class_id)
    )
    waitlist_classes = []

    # Loop through the class_id in response 
    for item in enrollment_data['Items']:
        class_id = item['class_id']
        # get details of particular classes in class_response
        class_response = class_table.get_item(
            Key={'id': class_id}  
        )
        if 'Item' in class_response:
            waitlist_class = class_response['Item']
            # logic to only show classes which the student is waitlist in
            if waitlist_class.get('current_enroll') > waitlist_class.get('max_enroll') and item['placement'] > waitlist_class['max_enroll']:
                department = department_table.get_item(Key={'id': waitlist_class['department_id']}).get('Item')
                waitlist_classes.append({
                    "id": waitlist_class.get('id'),
                    "student_id": item['student_id'],
                    "department_name": department.get('name'),
                    "course_code": waitlist_class.get('course_code'),
                    "section_number": waitlist_class.get('section_number'),
                    "class_name": waitlist_class.get('name'),
                    "current_enroll": waitlist_class.get('current_enroll'),
                    "waitlist_placement" : item['placement'] - waitlist_class['max_enroll']
                })
    # Construct the final response
    final_response = {"Waitlist": waitlist_classes}
    return {"Waitlist": final_response}

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
    reorder_placement_dynamodb(enrollment_data['placement'], class_id)
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

def reorder_placement_dynamodb(placement, class_id):
    enrollment_table = dynamodb.Table('enrollment')
    class_table = dynamodb.Table('class')

    # Query to get all enrollments for the class with placement greater than the specified placement
    response = enrollment_table.query(
        IndexName='ClassIndex',  # Replace with your GSI name
        KeyConditionExpression=Key('class_id').eq(class_id) & Key('placement').gt(placement)
    )
    items = response['Items']
    print(items)

    # Update placements
    for item in items:
        new_placement = item['placement'] - 1
        enrollment_table.update_item(
            Key={
                 "class_id": item['class_id'],# Assume 'enrollment_id' is the primary key
                 "student_id" : item['student_id']
            },
            UpdateExpression='SET placement = :val',
            ExpressionAttributeValues={
                ':val': new_placement
            }
        )

    # Decrement current enrollment in the class table
    class_table.update_item(
        Key={
            'id': class_id  
        },
        UpdateExpression='ADD current_enroll :dec',
        ExpressionAttributeValues={
            ':dec': -1
        }
    )
