from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from auth_database import AuthDatabase
import re

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')
auth_db = AuthDatabase()

def require_auth(f):
    """Decorator to require authentication for routes"""
    from functools import wraps
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'session_token' not in session:
            return redirect(url_for('auth.login'))
        
        is_valid, user_data = auth_db.validate_session(session['session_token'])
        if not is_valid:
            session.clear()
            flash('Your session has expired. Please log in again.', 'warning')
            return redirect(url_for('auth.login'))
        
        return f(user_data=user_data, *args, **kwargs)
    return decorated_function


def require_auth_api(f):
    """Decorator for API routes that returns JSON instead of redirecting"""
    from functools import wraps
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'session_token' not in session:
            return jsonify({'error': 'Authentication required', 'redirect': url_for('auth.login')}), 401
        
        is_valid, user_data = auth_db.validate_session(session['session_token'])
        if not is_valid:
            session.clear()
            return jsonify({'error': 'Session expired', 'redirect': url_for('auth.login')}), 401
        
        return f(user_data=user_data, *args, **kwargs)
    return decorated_function

@auth_bp.route('/signup', methods=['GET', 'POST'])
def signup():
    """User registration page"""
    if request.method == 'GET':
        # Generate a suggested strong password
        suggested_password = auth_db.generate_strong_password()
        return render_template('auth/signup.html', suggested_password=suggested_password)
    
    email = request.form.get('email', '').strip()
    password = request.form.get('password', '')
    use_suggested = request.form.get('use_suggested', False)
    
    if not email or not password:
        flash('Email and password are required', 'error')
        return redirect(url_for('auth.signup'))
    
    # Validate email format
    email_pattern = r'^[a-zA-Z0-9._%+-]+@andrew\.com$'
    if not re.match(email_pattern, email):
        flash('Please use a valid corporate email address', 'error')
        return redirect(url_for('auth.signup'))
    
    # Create user account
    success, message = auth_db.create_user(email, password)
    
    if success:
        flash('Account created successfully! Please log in.', 'success')
        return redirect(url_for('auth.login'))
    else:
        flash(message, 'error')
        return redirect(url_for('auth.signup'))

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """User login page"""
    if request.method == 'GET':
        return render_template('auth/login.html')
    
    email = request.form.get('email', '').strip()
    password = request.form.get('password', '')
    
    if not email or not password:
        flash('Email and password are required', 'error')
        return redirect(url_for('auth.login'))
    
    # Validate email domain for security
    if not email.endswith('@andrew.com'):
        flash('Invalid corporate email address', 'error')
        return redirect(url_for('auth.login'))
    
    # Authenticate user
    success, message, user_id = auth_db.authenticate_user(email, password)
    
    if success:
        # Create session
        session_token = auth_db.create_session(user_id)
        if session_token:
            session['session_token'] = session_token
            flash('Login successful!', 'success')
            return redirect(url_for('core.index'))
        else:
            flash('Failed to create session. Please try again.', 'error')
    else:
        flash(message, 'error')
    
    return redirect(url_for('auth.login'))

@auth_bp.route('/logout')
def logout():
    """User logout"""
    if 'session_token' in session:
        auth_db.logout_session(session['session_token'])
        session.clear()
        flash('You have been logged out successfully.', 'success')
    
    return redirect(url_for('auth.login'))

@auth_bp.route('/check-password-strength', methods=['POST'])
def check_password_strength():
    """API endpoint to check password strength"""
    password = request.json.get('password', '')
    is_valid, message = auth_db.validate_password_strength(password)
    
    return jsonify({
        'is_valid': is_valid,
        'message': message,
        'strength_score': calculate_password_score(password)
    })

@auth_bp.route('/generate-password', methods=['POST'])
def generate_password():
    """API endpoint to generate a new strong password"""
    length = request.json.get('length', 16)
    if length < 12:
        length = 12
    if length > 32:
        length = 32
    
    password = auth_db.generate_strong_password(length)
    is_valid, message = auth_db.validate_password_strength(password)
    
    return jsonify({
        'password': password,
        'is_valid': is_valid,
        'message': message,
        'strength_score': calculate_password_score(password)
    })

def calculate_password_score(password):
    """Calculate password strength score (0-100)"""
    score = 0
    
    # Length score (max 25 points)
    if len(password) >= 12:
        score += 15
    if len(password) >= 16:
        score += 10
    
    # Character variety (max 40 points)
    if any(c.islower() for c in password):
        score += 10
    if any(c.isupper() for c in password):
        score += 10
    if any(c.isdigit() for c in password):
        score += 10
    if any(c in "!@#$%^&*()-_+=[]{}|;:,.<>?" for c in password):
        score += 10
    
    # Complexity bonus (max 35 points)
    unique_chars = len(set(password))
    if unique_chars >= 8:
        score += 15
    if unique_chars >= 12:
        score += 10
    
    # No common patterns
    if not any(pattern in password.lower() for pattern in ['123', 'abc', 'password', 'andrew']):
        score += 10
    
    return min(score, 100)
