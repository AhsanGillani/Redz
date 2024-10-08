import os
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from tempfile import NamedTemporaryFile
from Fileapi.firebase_utils import insert_data_batch_to_firestore
from concurrent.futures import ThreadPoolExecutor
from firebase_admin import firestore

# Reference to the Firestore DB
db = firestore.client()

class FilePathUploadView(APIView):

    def post(self, request, format=None):
        file_url_or_path = request.data.get('file_path')
        if not file_url_or_path:
            return Response({'error': 'No file URL or path provided'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            logging.info(f"Received file path: {file_url_or_path}")
            file_path = self.get_file(file_url_or_path)
            self.process_file(file_path)
            if os.path.exists(file_path):
                os.remove(file_path)  # Clean up the temporary file if it's a downloaded file
            return Response({'success': 'File processed and data inserted into the database'}, status=status.HTTP_201_CREATED)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def get_file(self, file_url_or_path):
        if file_url_or_path.startswith(('http://', 'https://')):
            return self.download_file(file_url_or_path)
        else:
            if os.path.exists(file_url_or_path):
                return file_url_or_path
            else:
                raise Exception(f"File not found: {file_url_or_path}")

    def download_file(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            temp_file = NamedTemporaryFile(delete=False, suffix='.csv')
            temp_file.write(response.content)
            temp_file.close()
            return temp_file.name
        else:
            raise Exception(f"Failed to download file: {response.status_code}")

    def process_file(self, file_path):
        chunk_size = 1000  # Adjust the chunk size as needed
        with ThreadPoolExecutor() as executor:
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                # Convert the Date column to datetime
                chunk['Date'] = pd.to_datetime(chunk['Date'], errors='coerce')
                chunk = chunk.dropna(subset=['Break In', 'Break Out', 'Clock In', 'Clock Out'], how='all')

                data_list = []
                for _, row in chunk.iterrows():
                    employee_id = int(row['Employee ID']) if not pd.isna(row['Employee ID']) else None
                    csv_date = row['Date']

                    # Check for existing document based on Employee ID and the date part of dateTime
                    if employee_id and csv_date:
                        # Get the documents where employeeID matches
                        docs = db.collection('Attendance').where('employeeID', '==', employee_id).stream()
                        

                        record_exists = False
                        doc_id = None
                        for doc in docs:
                            doc_data = doc.to_dict()
                            if 'dateTime' in doc_data:
                                # Extract date part from dateTime
                                firebase_checkin_date = doc_data['dateTime'].date()
                                if firebase_checkin_date == csv_date.date():  # Compare only the date part
                                    record_exists = True
                                    doc_id = doc.id
                                    break
                        
                        if record_exists and doc_id:
                            # Update existing record
                            db.collection('Attendance').document(doc_id).update(self.prepare_data(row))
                            logging.info(f"Updated record for employee ID {employee_id} on {csv_date}.")
                        else:
                            # Insert new record
                            data_list.append(self.prepare_data(row))

                # Batch insert data if no duplicates found
                if data_list:
                    executor.submit(insert_data_batch_to_firestore, 'Attendance', data_list)

    def prepare_data(self, row):
        # Ensure Clock In, Clock Out, Break In, and Break Out are strings and add ":00" if seconds are missing
        clock_in_str = str(row['Clock In']) if pd.notna(row['Clock In']) else None
        clock_out_str = str(row['Clock Out']) if pd.notna(row['Clock Out']) else None
        break_in_str = str(row['Break In']) if pd.notna(row['Break In']) else None
        break_out_str = str(row['Break Out']) if pd.notna(row['Break Out']) else None
        print(clock_in_str)

        if clock_in_str and (len(clock_in_str) == 4 or len(clock_in_str) == 5):  # If format is 'HH:MM'
            clock_in_str += ":00"
            print(clock_in_str)
        if clock_out_str and (len(clock_out_str) == 4 or len(clock_out_str) == 5):  # If format is 'HH:MM'
            clock_out_str += ":00"
        if break_in_str and (len(break_in_str) == 4 or len(break_in_str) == 5):  # If format is 'HH:MM'
            break_in_str += ":00"
        if break_out_str and (len(break_out_str) == 4 or len(break_out_str) == 5):  # If format is 'HH:MM'
            break_out_str += ":00"

        try:
            # Convert Clock In, Clock Out, Break In, and Break Out from string to datetime objects
            clock_in_time = datetime.strptime(clock_in_str, '%H:%M:%S') if clock_in_str else None
            clock_out_time = datetime.strptime(clock_out_str, '%H:%M:%S') if clock_out_str else None
            break_in_time = datetime.strptime(break_in_str, '%H:%M:%S') if break_in_str else None
            break_out_time = datetime.strptime(break_out_str, '%H:%M:%S') if break_out_str else None
        except ValueError as e:
            logging.error(f"Error parsing time for row: {row}, error: {e}")
            clock_in_time = None
            clock_out_time = None
            break_in_time = None
            break_out_time = None

        # Combine Date and Time into a single datetime object, then subtract 5 hours
        if pd.notna(row['Date']) and clock_in_time:
            server_time_checkin = datetime.combine(row['Date'].date(), clock_in_time.time()) - timedelta(hours=5)
        else:
            server_time_checkin = None

        if pd.notna(row['Date']) and clock_out_time:
            server_time_checkout = datetime.combine(row['Date'].date(), clock_out_time.time()) - timedelta(hours=5)
        else:
            server_time_checkout = None

        if pd.notna(row['Date']) and break_in_time:
            break_start_time = datetime.combine(row['Date'].date(), break_in_time.time()) - timedelta(hours=5)
        else:
            break_start_time = None

        if pd.notna(row['Date']) and break_out_time:
            break_end_time = datetime.combine(row['Date'].date(), break_out_time.time()) - timedelta(hours=5)
        else:
            break_end_time = None




        # Calculate First Half Deduction
        first_half_deduction = 0
        #print(((server_time_checkin + timedelta(hours=5)).time()))
        if ((server_time_checkin !=None) and (((server_time_checkin + timedelta(hours=5)).time() > datetime.strptime("09:10:00", "%H:%M:%S").time())  and ((server_time_checkin + timedelta(hours=5)).time() < datetime.strptime("13:00:00", "%H:%M:%S").time()))) :
            server_time_checkin_new=(server_time_checkin + timedelta(hours=5))
            print("why not open the condition?")
            minutes_late = (server_time_checkin_new - datetime.combine(server_time_checkin_new.date(), datetime.strptime("09:00:00", "%H:%M:%S").time())).total_seconds() / 60
                        
        # Grace period is already considered
            if minutes_late > 10:  
                first_half_deduction = int((minutes_late - 11) // 30) +1
                first_half_deduction=first_half_deduction  * 30

                print("Yesy this is working")
            #if the user checkin after 1 PM so we need to deduct the first half slot 
        elif(server_time_checkin !=None ) and (((server_time_checkin + timedelta(hours=5)).time() >  datetime.strptime("13:00:00", "%H:%M:%S").time())):
            first_half_deduction=240

        


                    
                            
                            

         # Calculate Second Half Deduction

        second_half_deduction = 0
        if (server_time_checkin !=None) and (server_time_checkin + timedelta(hours=5)).time() == datetime.strptime("14:10:00", "%H:%M:%S").time()  and (server_time_checkin + timedelta(hours=5)).time() < datetime.strptime("18:00:00", "%H:%M:%S").time() :
            first_half_deduction=240
            print("yess secon half 1 is working")

        if (server_time_checkin !=None) and (server_time_checkin + timedelta(hours=5)).time() > datetime.strptime("14:10:00", "%H:%M:%S").time()  and (server_time_checkin + timedelta(hours=5)).time() < datetime.strptime("18:00:00", "%H:%M:%S").time() :
            server_time_checkin_new=(server_time_checkin + timedelta(hours=5))
            first_half_deduction=240
            minutes_late = (server_time_checkin_new - datetime.combine(server_time_checkin_new.date(), datetime.strptime("14:00:00", "%H:%M:%S").time())).total_seconds() / 60
                        
            if minutes_late > 10:
              # Grace period is already considered
                second_half_deduction = ((int(minutes_late) - 11) // 30 + 1) * 30
                          
                    
                    
        if break_end_time and (break_end_time + timedelta(hours=5)).time() > datetime.strptime("14:10:00", "%H:%M:%S").time():
            break_end_time_new=(break_end_time + timedelta(hours=5))   
            minutes_late = (break_end_time_new - datetime.combine(break_end_time_new.date(), datetime.strptime("14:00:00", "%H:%M:%S").time())).total_seconds() / 60
            if minutes_late > 10:
                second_half_deduction = ((int(minutes_late-11)) // 30 + 1) * 30
                      


        if break_start_time and  (break_start_time + timedelta(hours=5)).time()< datetime.strptime("13:00:00", "%H:%M:%S").time():
            break_start_time_new=(break_start_time + timedelta(hours=5))
            minutes_early = (datetime.combine(break_start_time_new.date(), datetime.strptime("13:00:00", "%H:%M:%S").time()) - break_start_time_new).total_seconds() / 60
            print(minutes_early)
            print(first_half_deduction)
            first_half_deduction += ((int(minutes_early -1)) // 30 + 1) * 30
            print('Break earlier debug',first_half_deduction)

        if(server_time_checkin ==None ):
            first_half_deduction=240
                        

        if server_time_checkout and  (server_time_checkout + timedelta(hours=5)).time()< datetime.strptime("18:00:00", "%H:%M:%S").time():
            server_time_checkout_new=(server_time_checkout + timedelta(hours=5))
            minutes_early = (datetime.combine(server_time_checkout_new.date(), datetime.strptime("18:00:00", "%H:%M:%S").time()) - server_time_checkout_new).total_seconds() / 60
            print(minutes_early)
            print(second_half_deduction)
            second_half_deduction += ((int(minutes_early -1)) // 30 + 1) * 30
            print('i add +1',second_half_deduction)
        elif(server_time_checkout==None):
            second_half_deduction=240

                    # Add a condition to ensure no deductions are applied when times are within limits
        if break_end_time and (break_end_time + timedelta(hours=5)).time() <= datetime.strptime("14:10:00", "%H:%M:%S").time():
                        
            second_half_deduction = second_half_deduction +0

        # Add a condition to ensure no deductions are applied when times are within limits                    

        if((server_time_checkin!=None) and (server_time_checkin + timedelta(hours=5)).time() <datetime.strptime("13:00:00", "%H:%M:%S").time()  and break_end_time==None and break_end_time ==None):
            second_half_deduction=240

        elif ((server_time_checkin!=None) and (server_time_checkin + timedelta(hours=5)).time() <datetime.strptime("13:00:00", "%H:%M:%S").time() and break_end_time ==None) :
            second_half_deduction = 240
                    
        if server_time_checkout and (server_time_checkout + timedelta(hours=5)).time() >= datetime.strptime("18:00:00", "%H:%M:%S").time():
                        
            second_half_deduction = second_half_deduction +0

       
    





            #extra time calculation
        extratime=0
        if server_time_checkout and  (server_time_checkout + timedelta(hours=5)).time()> datetime.strptime("18:00:00", "%H:%M:%S").time():
            server_time_checkout_new=(server_time_checkout + timedelta(hours=5))
            minutes_early = ( server_time_checkout_new - datetime.combine(server_time_checkout_new.date(), datetime.strptime("18:00:00", "%H:%M:%S").time()) ).total_seconds() / 60
            print(minutes_early)
            extratime += ((int(minutes_early )) // 30 ) * 30
                    




        total_time_str = str(row['Total Hours']) if not pd.isna(row['Total Hours']) else None
        if total_time_str:
            hours, minutes = map(int, total_time_str.split(':'))
            total_time_double = hours + minutes / 60.0
            total_time_str = f"{total_time_double:.2f}"

        available_time_without_break = str(row['Worked Hours']) if not pd.isna(row['Worked Hours']) else None
        if available_time_without_break:
            hours, minutes = map(int, available_time_without_break.split(':'))
            available_time_double = hours + minutes / 60.0
            available_time_without_break = f"{available_time_double:.2f}"

        # Prepare and return the data object for Firestore
        return {
            'break_start_time': break_start_time,
            'break_end_time': break_end_time,
            'total_time': total_time_str,
            'total_break_time': str(row['Break Hours']) if not pd.isna(row['Break Hours']) else None,
            'dateTime': row['Date'],
            'ServertimeCheckin': server_time_checkin,
            'ServertimeCheckout': server_time_checkout,
            'month': row['Date'].strftime('%m/%Y') if not pd.isna(row['Date']) else None,
            'timestamp': row['Date'],
            'totalLeaves': int(row['Total Leaves']) if not pd.isna(row['Total Leaves']) else 0,
            #'remainingLeaves': int(row['Remaining']) if not pd.isna(row['Remaining']) else 0,
            'extraTime': extratime,
            'employeeID': int(row['Employee ID']) if not pd.isna(row['Employee ID']) else 0,
            'available_time_without_break': available_time_without_break,
            'firstHalfDeductions': first_half_deduction,
            'secondHalfDeductions': second_half_deduction
        }











