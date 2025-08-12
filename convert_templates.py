#!/usr/bin/env python3

import os
import re

# List of templates to convert
templates = [
    'dhl_express_audit_summary.html',
    'dhl_express_audit_results.html', 
    'dhl_express_invoice_details.html'
]

def convert_template(filepath):
    """Convert a standalone template to extend base.html"""
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract title from the existing title tag
    title_match = re.search(r'<title>(.*?)</title>', content)
    title = title_match.group(1) if title_match else 'DHL Express'
    
    # Remove everything before the body tag and extract styles
    style_match = re.search(r'<style>(.*?)</style>', content, re.DOTALL)
    styles = style_match.group(1) if style_match else ''
    
    # Find the content between body tags
    body_match = re.search(r'<body>(.*)</body>', content, re.DOTALL)
    if not body_match:
        print(f"No body content found in {filepath}")
        return
    
    body_content = body_match.group(1)
    
    # Remove the navbar section
    navbar_pattern = r'<nav class="navbar.*?</nav>'
    body_content = re.sub(navbar_pattern, '', body_content, flags=re.DOTALL)
    
    # Replace container with container-fluid and update structure
    body_content = re.sub(r'<div class="container mt-4">', '<div class="container-fluid">', body_content)
    
    # Remove script tags from body (we'll add them back)
    script_pattern = r'<script.*?</script>'
    scripts = re.findall(script_pattern, body_content, re.DOTALL)
    body_content = re.sub(script_pattern, '', body_content, flags=re.DOTALL)
    
    # Create new template content
    new_content = f"""{{% extends "base.html" %}}
{{% block title %}}{title}{{% endblock %}}

{{% block content %}}
<style>
{styles}
</style>

{body_content.strip()}

{''.join(scripts)}
{{% endblock %}}"""
    
    # Write the new content
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"Converted {filepath}")

# Convert all templates
for template in templates:
    filepath = os.path.join('templates', template)
    if os.path.exists(filepath):
        convert_template(filepath)
    else:
        print(f"Template {filepath} not found")

print("Template conversion complete!")
