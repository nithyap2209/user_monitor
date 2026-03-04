import csv
import io
from flask import request, make_response


def get_pagination_args(default_per_page=20):
    """Extract page and per_page from query string."""
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", default_per_page, type=int)
    per_page = min(per_page, 100)
    return page, per_page


def flash_errors(form):
    """Flash all form validation errors."""
    from flask import flash
    for field, errors in form.errors.items():
        for error in errors:
            flash(f"{getattr(form, field).label.text}: {error}", "danger")


def export_csv(rows, headers, filename="export.csv"):
    """Generate a CSV download response."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)

    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "text/csv"
    return response
