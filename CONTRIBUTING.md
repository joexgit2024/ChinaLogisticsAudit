# Contributing to DHL Invoice Audit Application

Thank you for your interest in contributing to the DHL Invoice Audit Application! This guide will help you get started.

## Development Setup

### Prerequisites
- Python 3.12+
- SQLite3
- Git

### Environment Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/dhl-audit-app.git
cd dhl-audit-app

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -r requirements-dev.txt
```

## Code Standards

### Python Style Guide
- Follow PEP 8 guidelines
- Use type hints where appropriate
- Maximum line length: 120 characters
- Use descriptive variable and function names

### Code Structure
```python
# Good example
def calculate_ocean_audit_variance(
    invoice_data: Dict[str, Any], 
    rate_card: Dict[str, Any]
) -> Dict[str, float]:
    """
    Calculate detailed variance analysis for ocean freight charges.
    
    Args:
        invoice_data: Parsed invoice information
        rate_card: Matching rate card data
        
    Returns:
        Dictionary containing variance analysis results
    """
    # Implementation here
    pass
```

### Error Handling
Always include comprehensive error handling:
```python
try:
    result = process_edi_file(file_path)
except EDIParsingError as e:
    logger.error(f"EDI parsing failed: {e}")
    return {"error": "Invalid EDI format"}
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    return {"error": "Processing failed"}
```

## Testing

### Running Tests
```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/test_edi_parser.py

# Run with coverage
python -m pytest --cov=app tests/
```

### Writing Tests
- Write unit tests for all new functions
- Include edge cases and error conditions
- Use descriptive test names

```python
def test_ocean_audit_undercharge_scenario():
    """Test that undercharge scenarios correctly pass audit"""
    invoice_data = {
        "invoice_no": "TEST001",
        "total_charges_usd": 1000.00
    }
    rate_card = {"expected_total": 1200.00}
    
    result = calculate_audit_status(invoice_data, rate_card)
    assert result["status"] == "PASS"
    assert result["variance"] == -200.00
```

## Database Changes

### Schema Migrations
When modifying database schema:
1. Create migration script in `migrations/` folder
2. Test migration on sample data
3. Document changes in commit message

### Adding New Models
```python
# app/models.py
class NewModel(db.Model):
    __tablename__ = 'new_table'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
```

## EDI Processing

### Adding New Transaction Sets
1. Research the EDI specification
2. Create parser in `app/edi_parser.py`
3. Add corresponding database models
4. Write comprehensive tests

### EDI Parsing Guidelines
- Handle missing segments gracefully
- Validate data types and formats
- Log parsing errors with context
- Support multiple EDI versions when possible

## Web Interface

### Template Structure
```html
<!-- templates/new_page.html -->
{% extends "base.html" %}

{% block title %}Page Title{% endblock %}

{% block content %}
<div class="container">
    <!-- Content here -->
</div>
{% endblock %}

{% block scripts %}
<script>
    // Page-specific JavaScript
</script>
{% endblock %}
```

### Form Handling
- Use Flask-WTF for form validation
- Include CSRF protection
- Provide user-friendly error messages

## API Development

### RESTful Endpoints
```python
@app.route('/api/invoices/<invoice_no>/audit', methods=['GET'])
def get_invoice_audit(invoice_no: str):
    """Get audit results for specific invoice"""
    try:
        audit_engine = AuditEngine()
        result = audit_engine.audit_invoice(invoice_no)
        return jsonify(result)
    except InvoiceNotFoundError:
        return jsonify({"error": "Invoice not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
```

### API Documentation
- Document all endpoints
- Include example requests/responses
- Specify error codes and messages

## Commit Guidelines

### Commit Message Format
```
type(scope): short description

Longer description of changes if needed.

- List specific changes
- Include any breaking changes
- Reference issue numbers
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code formatting changes
- `refactor`: Code refactoring
- `test`: Adding/updating tests
- `chore`: Build/tooling changes

### Examples
```
feat(audit): add ocean freight FCL audit support

- Implement container-based pricing calculation
- Add fuzzy city matching for rate cards
- Support 20ft and 40ft container types
- Include undercharge logic for favorable pricing

Fixes #123
```

## Pull Request Process

### Before Submitting
- [ ] All tests pass
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] No sensitive data in commit
- [ ] Branch is up to date with main

### PR Description Template
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No console errors
```

## Security Considerations

### Data Protection
- Never commit sensitive data (API keys, passwords)
- Sanitize user inputs
- Use parameterized queries
- Validate file uploads

### EDI File Handling
- Validate file size limits
- Check file types
- Scan for malicious content
- Store uploads securely

## Performance Guidelines

### Database Queries
- Use appropriate indexes
- Avoid N+1 query problems
- Use pagination for large datasets
- Cache frequently accessed data

### File Processing
- Stream large files instead of loading into memory
- Process files asynchronously when possible
- Provide progress feedback for long operations

## Documentation

### Code Comments
- Explain complex business logic
- Document EDI segment meanings
- Include examples for unclear functions

### API Documentation
- Keep OpenAPI/Swagger spec updated
- Include example payloads
- Document error responses

## Getting Help

### Resources
- Check existing issues on GitHub
- Review project documentation
- Ask questions in team chat

### Reporting Issues
1. Check if issue already exists
2. Provide minimal reproduction case
3. Include environment details
4. Attach relevant logs/screenshots

## Recognition

Contributors will be acknowledged in:
- README.md contributors section
- Release notes for significant contributions
- Team recognition programs

Thank you for contributing to the DHL Invoice Audit Application!
