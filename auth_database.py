import sqlite3
import hashlib
import secrets
import string
from datetime import datetime

class AuthDatabase:
    def __init__(self, db_path='dhl_audit.db'):
        self.db_path = db_path
        self.init_auth_tables()
    
    def init_auth_tables(self):
        """Initialize user authentication tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                failed_attempts INTEGER DEFAULT 0,
                locked_until TIMESTAMP
            )
        ''')
        
        # Create user sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                session_token TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def validate_corporate_email(self, email):
        """Validate that email belongs to andrew.com domain"""
        return email.lower().endswith('@andrew.com')
    
    def generate_strong_password(self, length=16):
        """Generate a strong password with mixed characters"""
        # Ensure at least one character from each category
        lowercase = string.ascii_lowercase
        uppercase = string.ascii_uppercase
        digits = string.digits
        special_chars = "!@#$%^&*()-_+=[]{}|;:,.<>?"
        
        # Start with one from each category
        password = [
            secrets.choice(lowercase),
            secrets.choice(uppercase),
            secrets.choice(digits),
            secrets.choice(special_chars)
        ]
        
        # Fill the rest randomly
        all_chars = lowercase + uppercase + digits + special_chars
        for _ in range(length - 4):
            password.append(secrets.choice(all_chars))
        
        # Shuffle the password list
        secrets.SystemRandom().shuffle(password)
        return ''.join(password)
    
    def validate_password_strength(self, password):
        """Validate password meets complexity requirements"""
        if len(password) < 12:
            return False, "Password must be at least 12 characters long"
        
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(c in "!@#$%^&*()-_+=[]{}|;:,.<>?" for c in password)
        
        if not all([has_upper, has_lower, has_digit, has_special]):
            return False, "Password must contain uppercase, lowercase, numbers, and special characters"
        
        return True, "Password meets complexity requirements"
    
    def hash_password(self, password):
        """Hash password with salt"""
        salt = secrets.token_hex(32)
        password_hash = hashlib.pbkdf2_hmac('sha256', 
                                          password.encode('utf-8'), 
                                          salt.encode('utf-8'), 
                                          100000)
        return password_hash.hex(), salt
    
    def verify_password(self, password, stored_hash, salt):
        """Verify password against stored hash"""
        password_hash = hashlib.pbkdf2_hmac('sha256',
                                          password.encode('utf-8'),
                                          salt.encode('utf-8'),
                                          100000)
        return password_hash.hex() == stored_hash
    
    def create_user(self, email, password):
        """Create a new user account"""
        if not self.validate_corporate_email(email):
            return False, "Only corporate email addresses are allowed"
        
        is_valid, message = self.validate_password_strength(password)
        if not is_valid:
            return False, message
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if email already exists
            cursor.execute("SELECT id FROM users WHERE email = ?", (email.lower(),))
            if cursor.fetchone():
                return False, "Email address already registered"
            
            # Hash password and create user
            password_hash, salt = self.hash_password(password)
            cursor.execute('''
                INSERT INTO users (email, password_hash, salt)
                VALUES (?, ?, ?)
            ''', (email.lower(), password_hash, salt))
            
            conn.commit()
            return True, "Account created successfully"
            
        except sqlite3.Error as e:
            return False, f"Database error: {str(e)}"
        finally:
            conn.close()
    
    def authenticate_user(self, email, password):
        """Authenticate user login"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT id, password_hash, salt, is_active, failed_attempts, locked_until
                FROM users WHERE email = ?
            ''', (email.lower(),))
            
            user = cursor.fetchone()
            if not user:
                return False, "Invalid email or password", None
            
            user_id, stored_hash, salt, is_active, failed_attempts, locked_until = user
            
            # Check if account is active
            if not is_active:
                return False, "Account is deactivated", None
            
            # Check if account is locked
            if locked_until and datetime.now() < datetime.fromisoformat(locked_until):
                return False, "Account is temporarily locked due to failed attempts", None
            
            # Verify password
            if self.verify_password(password, stored_hash, salt):
                # Reset failed attempts and update last login
                cursor.execute('''
                    UPDATE users 
                    SET failed_attempts = 0, locked_until = NULL, last_login = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (user_id,))
                conn.commit()
                return True, "Login successful", user_id
            else:
                # Increment failed attempts
                new_failed_attempts = failed_attempts + 1
                locked_until = None
                
                # Lock account after 5 failed attempts for 30 minutes
                if new_failed_attempts >= 5:
                    from datetime import timedelta
                    locked_until = (datetime.now() + timedelta(minutes=30)).isoformat()
                
                cursor.execute('''
                    UPDATE users 
                    SET failed_attempts = ?, locked_until = ?
                    WHERE id = ?
                ''', (new_failed_attempts, locked_until, user_id))
                conn.commit()
                
                return False, "Invalid email or password", None
                
        except sqlite3.Error as e:
            return False, f"Database error: {str(e)}", None
        finally:
            conn.close()
    
    def create_session(self, user_id):
        """Create a new user session"""
        session_token = secrets.token_urlsafe(32)
        from datetime import datetime, timedelta
        expires_at = datetime.now() + timedelta(hours=24)  # 24 hour session
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO user_sessions (user_id, session_token, expires_at)
                VALUES (?, ?, ?)
            ''', (user_id, session_token, expires_at.isoformat()))
            conn.commit()
            return session_token
        except sqlite3.Error:
            return None
        finally:
            conn.close()
    
    def validate_session(self, session_token):
        """Validate user session token"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT u.id, u.email, s.expires_at
                FROM user_sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.session_token = ? AND s.is_active = 1 AND u.is_active = 1
            ''', (session_token,))
            
            result = cursor.fetchone()
            if not result:
                return False, None
            
            user_id, email, expires_at = result
            
            # Check if session has expired
            if datetime.now() > datetime.fromisoformat(expires_at):
                # Deactivate expired session
                cursor.execute('''
                    UPDATE user_sessions SET is_active = 0 WHERE session_token = ?
                ''', (session_token,))
                conn.commit()
                return False, None
            
            return True, {'id': user_id, 'email': email}
            
        except sqlite3.Error:
            return False, None
        finally:
            conn.close()
    
    def logout_session(self, session_token):
        """Logout user session"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE user_sessions SET is_active = 0 WHERE session_token = ?
            ''', (session_token,))
            conn.commit()
            return True
        except sqlite3.Error:
            return False
        finally:
            conn.close()
