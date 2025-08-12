#!/usr/bin/env python3
"""
Script to add authentication to all routes in app.py
"""

import re

def add_auth_to_routes():
    """Add @require_auth decorator to all app routes"""
    
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find all @app.route patterns and add @require_auth
    # Pattern: @app.route followed by function definition
    pattern = r'(@app\.route\([^)]+\))\s*\ndef\s+(\w+)\s*\([^)]*\):'
    
    def replace_route(match):
        route_decorator = match.group(1)
        function_name = match.group(2)
        
        # Skip auth routes - they shouldn't be protected
        if function_name in ['login', 'signup', 'logout', 'check_password_strength', 'generate_password']:
            return match.group(0)
        
        # Check if already has user_data parameter
        full_match = match.group(0)
        if 'user_data=None' in full_match:
            # Already has auth, just add decorator
            return f"{route_decorator}\n@require_auth\n{full_match[len(route_decorator)+1:]}"
        else:
            # Add both decorator and user_data parameter
            old_def = full_match[len(route_decorator)+1:]
            
            # Replace function definition to add user_data parameter
            if '(' in old_def and ')' in old_def:
                def_start = old_def.find('(')
                def_end = old_def.find(')')
                params = old_def[def_start+1:def_end].strip()
                
                if params:
                    new_params = f"{params}, user_data=None"
                else:
                    new_params = "user_data=None"
                
                new_def = old_def[:def_start+1] + new_params + old_def[def_end:]
                return f"{route_decorator}\n@require_auth\n{new_def}"
            
        return match.group(0)
    
    # Apply the replacements
    new_content = re.sub(pattern, replace_route, content, flags=re.DOTALL)
    
    # Write back to file
    with open('app.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("✅ Authentication added to all routes in app.py")

if __name__ == "__main__":
    add_auth_to_routes()
