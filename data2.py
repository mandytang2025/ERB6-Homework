import tkinter as tk
from tkinter import filedialog, messagebox
import csv
import psycopg2
import os
import re
from datetime import datetime, date, time, timedelta

class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            dbname="dishdb",
            user="postgres",
            password="jkl"
        )
        self.cur = self.conn.cursor()
    
    def import_csv(self, table_name, file_path):
        try:
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Remove 'id' column if present
                if 'id' in headers:
                    id_index = headers.index('id')
                    headers.pop(id_index)
                
                columns = ', '.join(headers)
                placeholders = ', '.join(['%s'] * len(headers))
                
                # Clear table before import
                self.cur.execute(f"DELETE FROM {table_name}")
                # Reset sequence
                self.cur.execute(f"ALTER SEQUENCE {table_name}_id_seq RESTART WITH 1")
                
                # Insert data
                for row in reader:
                    # Remove id value if exists
                    if 'id' in headers:
                        row.pop(id_index)
                    self.cur.execute(
                        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                        row
                    )
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            raise e
    
    def export_csv(self, table_name, file_path):
        try:
            self.cur.execute(f"SELECT * FROM {table_name}")
            rows = self.cur.fetchall()
            
            with open(file_path, 'w', newline='') as f:
                writer = csv.writer(f)
                # Write headers
                writer.writerow([desc[0] for desc in self.cur.description])
                writer.writerows(rows)
            return True
        except Exception as e:
            raise e
    
    def close(self):
        self.cur.close()
        self.conn.close()

class GUI2(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Special Data Import")
        self.geometry("600x500")
        self.parent = parent
        
        # Variables to store file paths
        self.auth_user_file = None
        self.foodie_contact_file = None
        
        # Main container
        container = tk.Frame(self)
        container.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # GUI Elements
        self.label = tk.Label(container, text="Select CSV files and import to database", font=("Arial", 14))
        self.label.pack(pady=20)
        
        # Buttons
        self.btn_auth = tk.Button(
            container, 
            text="1. Select Authorized User.csv", 
            command=self.select_auth_user,
            width=25,
            height=2,
            bg="#e0e0ff",
            font=("Arial", 15)
        )
        self.btn_auth.pack(pady=10)
        
        self.btn_foodie = tk.Button(
            container, 
            text="2. Select Foodie.csv", 
            command=self.select_foodie_contact,
            width=25,
            height=2,
            bg="#e0ffe0",
            font=("Arial", 15)
        )
        self.btn_foodie.pack(pady=10)
        
        self.btn_import = tk.Button(
            container, 
            text="3. Validate and Import Data", 
            command=self.validate_and_import,
            width=25,
            height=2,
            bg="#ffe0e0",
            state=tk.DISABLED,
            font=("Arial", 15)
        )
        self.btn_import.pack(pady=10)
        
        # Status Frame
        status_frame = tk.Frame(container)
        status_frame.pack(fill=tk.X, pady=10)
        
        self.auth_status = tk.Label(status_frame, text="❌ auth_user.csv not selected", fg="red", font=("Arial", 10))
        self.auth_status.pack(anchor="w", padx=10)
        
        self.foodie_status = tk.Label(status_frame, text="❌ foodie_contact.csv not selected", fg="red", font=("Arial", 10))
        self.foodie_status.pack(anchor="w", padx=10)
        
        # Terminal output
        terminal_frame = tk.LabelFrame(container, text="Log Output")
        terminal_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.terminal = tk.Text(terminal_frame, height=10, state=tk.DISABLED, bg="#f0f0f0")
        self.terminal.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Scrollbar
        scrollbar = tk.Scrollbar(self.terminal)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.terminal.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.terminal.yview)
        
        # Tag configurations
        self.terminal.tag_config("success", foreground="green")
        self.terminal.tag_config("error", foreground="red")
        self.terminal.tag_config("info", foreground="blue")

    def log_message(self, message, tag=None):
        self.terminal.config(state=tk.NORMAL)
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.terminal.insert(tk.END, f"[{timestamp}] {message}\n", tag)
        self.terminal.see(tk.END)
        self.terminal.config(state=tk.DISABLED)

    def select_auth_user(self):
        file_path = filedialog.askopenfilename(title="Select auth_user.csv", filetypes=[("CSV files", "*.csv")])
        if file_path:
            self.auth_user_file = file_path
            self.auth_status.config(text=f"✅ {file_path.split('/')[-1]} selected", fg="green")
            self._update_import_button_state()
            self.log_message(f"Selected auth_user file: {file_path}", "info")

    def select_foodie_contact(self):
        file_path = filedialog.askopenfilename(title="Select foodie_contact.csv", filetypes=[("CSV files", "*.csv")])
        if file_path:
            self.foodie_contact_file = file_path
            self.foodie_status.config(text=f"✅ {file_path.split('/')[-1]} selected", fg="green")
            self._update_import_button_state()
            self.log_message(f"Selected foodie_contact file: {file_path}", "info")

    def _update_import_button_state(self):
        if self.auth_user_file and self.foodie_contact_file:
            self.btn_import.config(state=tk.NORMAL)
        else:
            self.btn_import.config(state=tk.DISABLED)

    def validate_and_import(self):
        self.log_message("Starting validation and import process...", "info")
        
        # Validate auth_user.csv
        auth_valid, auth_data, auth_errors = self.validate_auth_user()
        if not auth_valid:
            for error in auth_errors:
                self.log_message(f"AUTH ERROR: {error}", "error")
            messagebox.showerror("Validation Failed", "auth_user.csv validation failed. Check terminal for details.")
            return
        
        # Validate foodie_contact.csv
        foodie_valid, foodie_data, foodie_errors = self.validate_foodie_contact(auth_data)
        if not foodie_valid:
            for error in foodie_errors:
                self.log_message(f"FOODIE ERROR: {error}", "error")
            messagebox.showerror("Validation Failed", "foodie_contact.csv validation failed. Check terminal for details.")
            return
        
        # Import to database
        try:
            self.import_to_database(auth_data, foodie_data)
            self.log_message("Import completed successfully!", "success")
            messagebox.showinfo("Success", "Data imported successfully!")
            # Close both windows after successful import
            self.close_all_windows()
        except Exception as e:
            self.log_message(f"DATABASE ERROR: {str(e)}", "error")
            messagebox.showerror("Import Failed", f"Database import failed: {str(e)}")

    def validate_auth_user(self):
        errors = []
        data = []
        username_set = set()
        username_lower_set = set()  # For case-insensitive checks
        
        try:
            with open(self.auth_user_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # Sort rows by 'id' in ascending order
                try:
                    rows.sort(key=lambda row: int(row['id']))
                except (KeyError, ValueError) as e:
                    errors.append(f"Error sorting by ID: {str(e)}")
                
                for row in rows:
                    username = row['username']
                    username_lower = username.lower()  # For case-insensitive check
                    
                    # Check uniqueness - exact match
                    if username in username_set:
                        errors.append(f"Duplicate username: {username}")
                    
                    # Check uniqueness - case-insensitive
                    if username_lower in username_lower_set:
                        errors.append(f"Duplicate username (case-insensitive): {username}")
                    
                    username_set.add(username)
                    username_lower_set.add(username_lower)
                    
                    # Check required fields
                    required_fields = ['password', 'email', 'date_joined']
                    for field in required_fields:
                        if not row[field].strip():
                            errors.append(f"Missing {field} for user {username}")
                    
                    # ====== FIX: CASE-INSENSITIVE BOOLEAN VALIDATION ======
                    # Check boolean fields (accept both 'True/False' and 'TRUE/FALSE')
                    boolean_fields = ['is_superuser', 'is_staff', 'is_active']
                    for field in boolean_fields:
                        value = row[field].strip().upper()
                        if value not in ['TRUE', 'FALSE']:
                            # Also check for Python-style booleans
                            if value in ['TRUE', 'FALSE']:
                                row[field] = value  # Normalize to uppercase
                            else:
                                errors.append(f"Invalid {field} value '{row[field]}' for user {username}")
                    
                    # Handle empty names
                    row['first_name'] = row['first_name'] if row['first_name'].strip() else None
                    row['last_name'] = row['last_name'] if row['last_name'].strip() else None
                    
                    data.append(row)
            
            if not errors:
                self.log_message("auth_user.csv validation passed", "success")
            return (len(errors) == 0, data, errors)
        
        except Exception as e:
            errors.append(f"Error reading file: {str(e)}")
            return (False, None, errors)

    def validate_foodie_contact(self, auth_data):
        errors = []
        data = []
        foodie_name_set = set()
        
        # Create lowercase username set for case-insensitive matching
        auth_usernames_lower = {row['username'].lower() for row in auth_data}
        
        try:
            with open(self.foodie_contact_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # Sort rows by 'id' in ascending order
                try:
                    rows.sort(key=lambda row: int(row['id']))
                except (KeyError, ValueError) as e:
                    errors.append(f"Error sorting by ID: {str(e)}")
                
                for row in rows:
                    foodie_name = row['foodie_name']
                    
                    # Check uniqueness
                    if foodie_name in foodie_name_set:
                        errors.append(f"Duplicate foodie_name: {foodie_name}")
                    foodie_name_set.add(foodie_name)
                    
                    # Check required fields
                    required_fields = ['gender', 'age_range', 'occupation', 'live_district']
                    for field in required_fields:
                        if not row[field].strip():
                            errors.append(f"Missing {field} for foodie {foodie_name}")
                    
                    # Check boolean fields (accept both 'True/False' and 'TRUE/FALSE')
                    boolean_fields = [
                        'favor_chinese', 'favor_western', 'favor_veg', 'favor_organic',
                        'favor_japan', 'favor_korean', 'favor_thai', 'favor_seafood',
                        'favor_muslim', 'favor_no_beef', 'favor_no_pork', 'is_mvp'
                    ]
                    for field in boolean_fields:
                        value = row[field].strip().upper()
                        if value not in ['TRUE', 'FALSE']:
                            # Also check for Python-style booleans
                            if value in ['TRUE', 'FALSE']:
                                row[field] = value  # Normalize to uppercase
                            else:
                                errors.append(f"Invalid {field} value '{row[field]}' for foodie {foodie_name}")
                    
                    # Case-insensitive matching
                    if foodie_name.lower() not in auth_usernames_lower:
                        errors.append(f"Username {foodie_name} not found in auth_user for foodie {foodie_name}")
                    
                    data.append(row)
            
            if not errors:
                self.log_message("foodie_contact.csv validation passed", "success")
            return (len(errors) == 0, data, errors)
        
        except Exception as e:
            errors.append(f"Error reading file: {str(e)}")
            return (False, None, errors)

    def import_to_database(self, auth_data, foodie_data):
        conn = None
        try:
            # Connect to database
            conn = psycopg2.connect(
                host="localhost",
                dbname="dishdb",
                user="postgres",
                password="jkl"
            )
            cur = conn.cursor()
            self.log_message("Connected to database successfully", "info")
            
            # Delete existing records and reset sequences
            cur.execute("DELETE FROM foodie_contact;")
            cur.execute("ALTER SEQUENCE foodie_contact_id_seq RESTART WITH 1;")
            cur.execute("DELETE FROM auth_user;")
            cur.execute("ALTER SEQUENCE auth_user_id_seq RESTART WITH 1;")
            self.log_message("Cleared existing records and reset sequences", "info")
            
            # Create case-insensitive mapping of username to new ID
            username_id_map = {}
            
            # Import auth_user data
            for row in auth_data:
                # ====== FIX: CASE-INSENSITIVE BOOLEAN CONVERSION ======
                cur.execute("""
                    INSERT INTO auth_user (password, last_login, is_superuser, username, 
                        first_name, last_name, email, is_staff, is_active, date_joined)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                """, (
                    row['password'], row['last_login'], 
                    row['is_superuser'].strip().upper() == 'TRUE', 
                    row['username'], 
                    row['first_name'], row['last_name'], row['email'], 
                    row['is_staff'].strip().upper() == 'TRUE', 
                    row['is_active'].strip().upper() == 'TRUE', 
                    row['date_joined']
                ))
                new_id = cur.fetchone()[0]
                
                # Map lowercase username to ID
                username_id_map[row['username'].lower()] = new_id
            
            self.log_message(f"Imported {len(auth_data)} records to auth_user table", "info")
            
            # Import foodie_contact data
            for row in foodie_data:
                # Lookup user ID using lowercase username
                user_id = username_id_map.get(row['foodie_name'].lower())
                if not user_id:
                    raise ValueError(f"User ID not found for {row['foodie_name']}")
                
                cur.execute("""
                    INSERT INTO foodie_contact (
                        foodie_name, updated_date, gender, age_range, occupation, live_district,
                        favor_chinese, favor_western, favor_veg, favor_organic, favor_japan, favor_korean,
                        favor_thai, favor_seafood, favor_muslim, favor_no_beef, favor_no_pork,
                        foodie_desc, foodie_photo, is_mvp, user_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['foodie_name'], row['updated_date'], 
                    row['gender'], row['age_range'], row['occupation'], row['live_district'],
                    row['favor_chinese'].strip().upper() == 'TRUE', 
                    row['favor_western'].strip().upper() == 'TRUE',
                    row['favor_veg'].strip().upper() == 'TRUE', 
                    row['favor_organic'].strip().upper() == 'TRUE',
                    row['favor_japan'].strip().upper() == 'TRUE', 
                    row['favor_korean'].strip().upper() == 'TRUE',
                    row['favor_thai'].strip().upper() == 'TRUE', 
                    row['favor_seafood'].strip().upper() == 'TRUE',
                    row['favor_muslim'].strip().upper() == 'TRUE', 
                    row['favor_no_beef'].strip().upper() == 'TRUE',
                    row['favor_no_pork'].strip().upper() == 'TRUE', 
                    row['foodie_desc'], 
                    row['foodie_photo'], 
                    row['is_mvp'].strip().upper() == 'TRUE', 
                    user_id
                ))
            self.log_message(f"Imported {len(foodie_data)} records to foodie_contact table", "info")
            
            # Commit changes and close connection
            conn.commit()
            cur.close()
            self.log_message("Database connection closed", "info")
            self.log_message("IMPORT OK - All operations completed successfully!", "success")
            
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.log_message(f"DATABASE ERROR: {e.pgerror}", "error")
            raise Exception(f"Database error: {e.pgerror}") from e
        finally:
            if conn:
                conn.close()
    
    def close_all_windows(self):
        """Close both the import window and the main application window"""
        self.destroy()  # Close current window
        self.parent.destroy()  # Close main application window

class MainApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Data Manager for Two-Dish-Rice")
        self.root.geometry("650x500")
        
        self.db = DatabaseManager()
        self.option_var = tk.IntVar(value=1)
        
        self.create_widgets()
    
    def create_widgets(self):
        # Create main frame with padding
        main_frame = tk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title label
        title_label = tk.Label(
            main_frame, 
            text="Data Management App", 
            font=("Arial", 20, "bold")
        )
        title_label.pack(pady=(0, 15))
        
        # Create option frame
        option_frame = tk.Frame(main_frame)
        option_frame.pack(fill=tk.X, pady=10)
        
        # Option label
        option_label = tk.Label(option_frame, text="Select Data Type:", font=("Arial", 12))
        option_label.pack(side=tk.LEFT, padx=(0, 10))
        
        # Create radio buttons for table selection
        tk.Radiobutton(
            option_frame, 
            text="Admin User", 
            variable=self.option_var, 
            value=1,
            font=("Arial", 15)
        ).pack(side=tk.LEFT, padx=5)
        
        tk.Radiobutton(
            option_frame, 
            text="Restaurant", 
            variable=self.option_var, 
            value=2,
            font=("Arial", 15)
        ).pack(side=tk.LEFT, padx=5)
        
        tk.Radiobutton(
            option_frame, 
            text="Authorized User/ Foodie", 
            variable=self.option_var, 
            value=3,
            font=("Arial", 15)
        ).pack(side=tk.LEFT, padx=5)
        
        # ====== FIXED: REORGANIZED FRAME STRUCTURE ======
        # Create container for buttons and instructions
        content_frame = tk.Frame(main_frame)
        content_frame.pack(fill=tk.BOTH, expand=True)
        
        # Button container with vertical centering
        button_container = tk.Frame(content_frame)
        button_container.pack(fill=tk.BOTH, expand=True, pady=20)
        
        # Add spacer above buttons
        spacer_top = tk.Frame(button_container, height=20)
        spacer_top.pack(fill=tk.X, expand=True)
        
        # Button frame
        button_frame = tk.Frame(button_container)
        button_frame.pack(fill=tk.X, pady=10)
        
        # Import button
        self.browse_btn = tk.Button(
            button_frame, 
            text="Import Data", 
            font=("Arial", 17, "bold"), 
            command=self.import_action,
            width=15, 
            height=4, 
            bg="#2196F3", 
            fg="white"
        )
        self.browse_btn.pack(pady=10)
        
        # Export button
        self.export_btn = tk.Button(
            button_frame, 
            text="Export Data", 
            font=("Arial", 17, "bold"), 
            command=self.export_action,
            width=15, 
            height=4, 
            bg="#4CAF50", 
            fg="white"
        )
        self.export_btn.pack(pady=10)
        
        # Add spacer below buttons
        spacer_bottom = tk.Frame(button_container, height=20)
        spacer_bottom.pack(fill=tk.X, expand=True)
        
        # ====== FIXED: INSTRUCTIONS SECTION ======
        # Create frame for instructions at the bottom
        instructions_frame = tk.Frame(content_frame)
        instructions_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))
        
        # Add important notes
        notes_text = (
            "Important Notes:\n"
            "1. Exported CSV files are stored in the same folder as this program\n"
            "2. For Special Operations (Foodie/Authorized User):\n"
            "   - Import requires both auth_user.csv and foodie_contact.csv\n"
            "   - Export will create both Authorized_User.csv and Foodie.csv"
        )
        
        notes_label = tk.Label(
            instructions_frame, 
            text=notes_text,
            fg="red",
            justify=tk.LEFT,
            font=("Arial", 9, "italic"),
            wraplength=550
        )
        notes_label.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))
        
        # Add basic instructions
        instructions = tk.Label(
            instructions_frame, 
            text="Instructions:\n1. Select data type\n2. Click Import to import data\n3. Click Export to export data",
            fg="gray",
            justify=tk.LEFT,
            font=("Arial", 10)
        )
        instructions.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = tk.Label(
            self.root, 
            textvariable=self.status_var, 
            bd=1, relief=tk.SUNKEN, 
            anchor=tk.W,
            font=("Arial", 10)
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def import_admin_user(self, file_path):
        # Updated to match the actual CSV structure
        expected_columns = ['id', 'admin_name', 'admin_photo', 'admin_desc', 'admin_email']
        errors = []
        rows = []
        seen_names = set()
        seen_emails = set()
        
        # Read and validate CSV
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                if header != expected_columns:
                    errors.append(f"Header mismatch. Expected: {expected_columns}, got: {header}")
                    messagebox.showerror("Header Error", "\n".join(errors))
                    return
                
                for i, row in enumerate(reader, start=2):
                    if len(row) != len(expected_columns):
                        errors.append(f"Row {i}: Incorrect number of columns")
                        continue
                    
                    row_dict = dict(zip(expected_columns, row))
                    name_val = row_dict['admin_name']
                    email_val = row_dict['admin_email']
                    desc_val = row_dict['admin_desc']
                    photo_val = row_dict['admin_photo']
                    
                    # Check uniqueness
                    if not name_val:
                        errors.append(f"Row {i}: admin_name is missing")
                    elif name_val in seen_names:
                        errors.append(f"Row {i}: Duplicate admin_name {name_val}")
                    else:
                        seen_names.add(name_val)
                    
                    if not email_val:
                        errors.append(f"Row {i}: admin_email is missing")
                    elif email_val in seen_emails:
                        errors.append(f"Row {i}: Duplicate admin_email {email_val}")
                    else:
                        seen_emails.add(email_val)
                    
                    # Handle empty description
                    if not desc_val.strip():
                        desc_val = None
                    
                    # Handle empty photo - provide default value
                    if not photo_val.strip() or photo_val.lower() in ['none', 'null']:
                        photo_val = 'default_admin.png'
                    elif photo_val:
                        ext = os.path.splitext(photo_val)[1].lower()
                        if ext not in ['.png', '.jpg', '.jpeg']:
                            errors.append(f"Row {i}: Invalid image extension '{ext}' for photo")
                    
                    rows.append((name_val, email_val, desc_val, photo_val))
        except Exception as e:
            errors.append(f"Error reading file: {str(e)}")
        
        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return
        
        # Import to database
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    cur.execute("DELETE FROM adminusers_adminuser")
                    cur.execute("ALTER SEQUENCE adminusers_adminuser_id_seq RESTART WITH 1")
                    query = """
                        INSERT INTO adminusers_adminuser 
                        (admin_name, admin_email, admin_desc, admin_photo)
                        VALUES (%s, %s, %s, %s)
                    """
                    cur.executemany(query, rows)
            messagebox.showinfo("Success", "Admin User data imported successfully!")
            self.status_var.set("Admin User data imported")
            self.root.destroy()
        except Exception as e:
            messagebox.showerror("Database Error", str(e))
            self.status_var.set(f"Error: {str(e)}")
    
    def import_restaurant(self, file_path):
        expected_columns = [
            "id", "restaurant_name", "list_date", "edit_date", "restaurant_photo_main", 
            "restaurant_area", "restaurant_district", "restaurant_street", "restaurant_address", 
            "fullday", "openhour_fullday", "closehour_fullday", 
            "afternoon", "openhour_afternoon", "closehour_afternoon", 
            "night", "openhour_night", "closehour_night", 
             "nightsnack", "openhour_nightsnack", "closehour_nightsnack", 
            "category_chinese", "category_western", "category_seafood", "category_veg", "category_japan", 
            "menu", 
            "menu_photo1", "menu_photo2", "menu_photo3", "menu_photo4", "menu_photo5", "menu_photo6", 
            "two_dish_price", "three_dish_price", "drink_price", "soup_price", 
            "payment_cash", "payment_octopus", "payment_alipayhk", "payment_wechatpay", "payment_payeme", 
            "dine_in", "takeaway", "takeaway_self", "takeaway_keeta", "takeaway_foodpanda", 
            "is_published", "discount_coupon"
        ]
        
        # Define field lists
        boolean_fields = [
            'fullday', 'afternoon', 'night', 'nightsnack',
            'category_chinese', 'category_western', 'category_seafood', 'category_veg', 'category_japan',
            'payment_cash', 'payment_octopus', 'payment_alipayhk', 'payment_wechatpay', 'payment_payeme',
            'dine_in', 'takeaway', 'takeaway_self', 'takeaway_keeta', 'takeaway_foodpanda',
            'is_published', 'discount_coupon'
        ]
        price_fields = ['two_dish_price', 'three_dish_price', 'drink_price', 'soup_price']
        photo_fields = [
            'restaurant_photo_main',
            'menu_photo1', 'menu_photo2', 'menu_photo3', 'menu_photo4', 'menu_photo5', 'menu_photo6'
        ]
        time_groups = [
            ('fullday', 'openhour_fullday', 'closehour_fullday'),
            ('afternoon', 'openhour_afternoon', 'closehour_afternoon'),
            ('night', 'openhour_night', 'closehour_night'),
            ('nightsnack', 'openhour_nightsnack', 'closehour_nightsnack')
        ]
        
        errors = []
        rows = []
        seen_names = set()
        
        # Read and validate CSV
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                if header != expected_columns:
                    errors.append(f"Header mismatch. Expected: {expected_columns}, got: {header}")
                    messagebox.showerror("Header Error", "\n".join(errors))
                    return
                
                for i, row in enumerate(reader, start=2):
                    if len(row) != len(expected_columns):
                        errors.append(f"Row {i}: Incorrect number of columns")
                        continue
                    
                    row_dict = dict(zip(expected_columns, row))
                    
                    # Normalize boolean fields
                    for field in boolean_fields:
                        value = row_dict[field].strip().upper()
                        if value in ['TRUE', 'T', '1']:
                            row_dict[field] = 'TRUE'
                        elif value in ['FALSE', 'F', '0']:
                            row_dict[field] = 'FALSE'
                        # Update the row to have normalized value
                        idx = expected_columns.index(field)
                        row[idx] = row_dict[field]
                    
                    # Validate restaurant_name
                    name_val = row_dict['restaurant_name']
                    if not name_val.strip():
                        errors.append(f"Row {i}: restaurant_name is missing")
                    elif name_val in seen_names:
                        errors.append(f"Row {i}: Duplicate restaurant_name {name_val}")
                    else:
                        seen_names.add(name_val)
                    
                    # Validate list_date
                    if not row_dict['list_date'].strip():
                        errors.append(f"Row {i}: list_date is missing")
                    else:
                        try:
                            dt = datetime.fromisoformat(row_dict['list_date'])
                        except ValueError:
                            errors.append(f"Row {i}: list_date has invalid ISO format")
                    
                    # Validate edit_date
                    if not row_dict['edit_date'].strip():
                        errors.append(f"Row {i}: edit_date is missing")
                    else:
                        try:
                            # Try ISO format first
                            dt = datetime.strptime(row_dict['edit_date'], "%Y-%m-%d")
                            # Convert to expected format
                            row_dict['edit_date'] = dt.strftime("%d/%m/%Y")
                            # Update the row
                            idx = expected_columns.index('edit_date')
                            row[idx] = row_dict['edit_date']
                        except ValueError:
                            try:
                                datetime.strptime(row_dict['edit_date'], "%d/%m/%Y")
                            except ValueError:
                                errors.append(f"Row {i}: edit_date has invalid format (expected dd/mm/yyyy)")
                    
                    # Validate boolean fields
                    for field in boolean_fields:
                        if row_dict[field] not in ['TRUE', 'FALSE']:
                            errors.append(f"Row {i}: {field} must be 'TRUE' or 'FALSE'")
                    
                    # Validate price fields
                    for field in price_fields:
                        if not row_dict[field].strip():
                            errors.append(f"Row {i}: {field} is missing")
                        else:
                            try:
                                float(row_dict[field])
                            except ValueError:
                                errors.append(f"Row {i}: {field} must be a number")
                    
                    # Validate photo fields
                    for field in photo_fields:
                        value = row_dict[field].strip()
                        if value in ['', 'None', 'null']:
                            continue
                        elif value:
                            ext = os.path.splitext(value)[1].lower()
                            if ext not in ['.png', '.jpg', '.jpeg']:
                                errors.append(f"Row {i}: {field} has invalid file extension")
                    
                    # Validate time groups
                    for group in time_groups:
                        flag_field, open_field, close_field = group
                        if row_dict[flag_field] == 'TRUE':
                            if not row_dict[open_field].strip() or not row_dict[close_field].strip():
                                errors.append(f"Row {i}: {open_field} and {close_field} are required when {flag_field} is TRUE")
                            else:
                                try:
                                    # Normalize time format to HH:MM
                                    open_time = row_dict[open_field].strip()
                                    close_time = row_dict[close_field].strip()
                                    
                                    # Add leading zero if needed
                                    if len(open_time.split(':')[0]) == 1:
                                        open_time = '0' + open_time
                                    if len(close_time.split(':')[0]) == 1:
                                        close_time = '0' + close_time
                                    
                                    # Update values
                                    row_dict[open_field] = open_time
                                    row_dict[close_field] = close_time
                                    
                                    # Update the row
                                    open_idx = expected_columns.index(open_field)
                                    close_idx = expected_columns.index(close_field)
                                    row[open_idx] = open_time
                                    row[close_idx] = close_time
                                    
                                    # Validate format
                                    time.fromisoformat(open_time)
                                    time.fromisoformat(close_time)
                                except ValueError:
                                    errors.append(f"Row {i}: {open_field} or {close_field} has invalid time format")
                    
                    # If no errors, add to rows
                    if not any(e.startswith(f"Row {i}:") for e in errors):
                        # Remove ID column
                        row_without_id = row[1:]
                        rows.append(row_without_id)
        except Exception as e:
            errors.append(f"Error reading file: {str(e)}")
        
        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return
        
        # Prepare data for import
        converted_rows = []
        insert_columns = expected_columns[1:]  # Skip ID column
        for row in rows:
            conv_row = {}
            
            # Convert list_date
            try:
                dt_list = datetime.fromisoformat(row[insert_columns.index('list_date')])
                conv_row['list_date'] = dt_list.astimezone().replace(tzinfo=None)
            except Exception:
                conv_row['list_date'] = None
            
            # Convert edit_date
            try:
                edit_date_str = row[insert_columns.index('edit_date')]
                conv_row['edit_date'] = datetime.strptime(edit_date_str, "%d/%m/%Y")
            except Exception:
                conv_row['edit_date'] = None
            
            # Convert menu - use original value even if empty
            menu_val = row[insert_columns.index('menu')]
            conv_row['menu'] = menu_val if menu_val != '' else ''
            
            # Convert boolean fields
            for field in boolean_fields:
                conv_row[field] = (row[insert_columns.index(field)] == 'TRUE')
            
            # Convert price fields
            for field in price_fields:
                conv_row[field] = float(row[insert_columns.index(field)])
            
            # Convert photo fields
            for field in photo_fields:
                value = row[insert_columns.index(field)].strip()
                if value in ['', 'None', 'null']:
                    conv_row[field] = 'None'
                else:
                    conv_row[field] = value
            
            # Convert time groups
            for group in time_groups:
                flag_field, open_field, close_field = group
                if conv_row[flag_field]:
                    open_time_str = row[insert_columns.index(open_field)]
                    close_time_str = row[insert_columns.index(close_field)]
                    
                    try:
                        conv_row[open_field] = time.fromisoformat(open_time_str)
                        conv_row[close_field] = time.fromisoformat(close_time_str)
                    except ValueError:
                        conv_row[open_field] = None
                        conv_row[close_field] = None
                else:
                    conv_row[open_field] = None
                    conv_row[close_field] = None
            
            # Add other fields
            for field in ['restaurant_name', 'restaurant_area', 'restaurant_district', 
                          'restaurant_street', 'restaurant_address']:
                conv_row[field] = row[insert_columns.index(field)]
            
            # Create tuple in correct order
            row_tuple = tuple(conv_row[col] for col in insert_columns)
            converted_rows.append(row_tuple)
        
        # Import to database
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    cur.execute("DELETE FROM listings_two_dish_rice")
                    cur.execute("ALTER SEQUENCE listings_two_dish_rice_id_seq RESTART WITH 1")
                    placeholders = ', '.join(['%s'] * len(insert_columns))
                    query = f"""
                        INSERT INTO listings_two_dish_rice ({', '.join(insert_columns)})
                        VALUES ({placeholders})
                    """
                    cur.executemany(query, converted_rows)
            messagebox.showinfo("Success", "Restaurant data imported successfully!")
            self.status_var.set("Restaurant data imported")
            self.root.destroy()
        except Exception as e:
            messagebox.showerror("Database Error", str(e))
            self.status_var.set(f"Error: {str(e)}")
    
    def import_action(self):
        option = self.option_var.get()
        
        if option == 3:
            gui2 = GUI2(self.root)
            self.status_var.set("Special Import GUI opened")
            return
        
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not file_path:
            return
        
        try:
            if option == 1:
                self.import_admin_user(file_path)
            elif option == 2:
                self.import_restaurant(file_path)
        except Exception as e:
            messagebox.showerror("Error", f"Import failed: {str(e)}")
            self.status_var.set(f"Error: {str(e)}")
    
    def export_action(self):
        option = self.option_var.get()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            if option == 1:
                filename = f"Adminuser_{timestamp}.csv"
                self.db.export_csv('adminusers_adminuser', filename)
                messagebox.showinfo("Success", f"Admin User data exported successfully!\nFile: {filename}")
                self.status_var.set(f"Admin User data exported to {filename}")
                self.root.destroy()
                
            elif option == 2:
                filename = f"Restaurant_{timestamp}.csv"
                self.db.export_csv('listings_two_dish_rice', filename)
                messagebox.showinfo("Success", f"Restaurant data exported successfully!\nFile: {filename}")
                self.status_var.set(f"Restaurant data exported to {filename}")
                self.root.destroy()
                
            elif option == 3:
                auth_filename = f"Authorized_User_{timestamp}.csv"
                self.db.export_csv('auth_user', auth_filename)
                
                foodie_filename = f"Foodie_{timestamp}.csv"
                self.db.export_csv('foodie_contact', foodie_filename)
                
                messagebox.showinfo(
                    "Success", 
                    f"Both tables exported successfully!\n"
                    f"Authorized User: {auth_filename}\n"
                    f"Foodie: {foodie_filename}"
                )
                self.status_var.set("Auth User and Foodie Contact data exported")
                self.root.destroy()
                
        except Exception as e:
            messagebox.showerror("Error", f"Export failed: {str(e)}")
            self.status_var.set(f"Error: {str(e)}")
    
    def run(self):
        self.root.mainloop()
        self.db.close()

if __name__ == "__main__":
    app = MainApp()
    app.run()