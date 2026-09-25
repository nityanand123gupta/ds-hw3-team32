#!/usr/bin/env python3
"""One-off script: renders REPORT.md into REPORT.pdf for Moodle submission.
Not part of the graded deliverables -- just a formatting convenience."""
import re

from fpdf import FPDF

SRC = "REPORT.md"
OUT = "REPORT.pdf"


class ReportPDF(FPDF):
    def header(self):
        pass

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 8, f"Page {self.page_no()}", align="C")


def clean_inline(s: str) -> str:
    s = re.sub(r"\*\*(.*?)\*\*", r"\1", s)
    s = re.sub(r"`([^`]*)`", r"\1", s)
    s = s.replace("->", "->").replace("’", "'")
    return s


def render_table(pdf: ReportPDF, rows):
    """Proper bordered grid: each cell wraps onto multiple lines instead of
    being truncated. Relative column widths come from the longest word in
    each column (not the longest whole cell), so wrapping is reasonable."""
    cleaned = [[clean_inline(c) for c in row] for row in rows]
    ncols = max(len(r) for r in cleaned)
    for r in cleaned:
        while len(r) < ncols:
            r.append("")

    page_w = pdf.w - pdf.l_margin - pdf.r_margin
    # weight columns by their average content length, but keep a floor so
    # narrow columns (e.g. a single word like "MPI") don't get squeezed to 0
    avg_len = [max(3, sum(len(r[i]) for r in cleaned) / len(cleaned)) for i in range(ncols)]
    total = sum(avg_len)
    col_w = [max(18, page_w * (a / total)) for a in avg_len]
    scale = page_w / sum(col_w)
    col_w = [w * scale for w in col_w]

    def draw_row(cells, bold):
        pdf.set_font("Helvetica", "B" if bold else "", 8.3)
        x0, y0 = pdf.l_margin, pdf.get_y()
        line_h = 4.2
        heights = []
        for i, text in enumerate(cells):
            lines = pdf.multi_cell(col_w[i], line_h, text, border=0, align="L",
                                    dry_run=True, output="LINES")
            heights.append(max(1, len(lines)) * line_h)
        row_h = max(heights)
        if y0 + row_h > pdf.page_break_trigger:
            pdf.add_page()
            y0 = pdf.get_y()
        x = x0
        for i, text in enumerate(cells):
            pdf.set_xy(x, y0)
            pdf.multi_cell(col_w[i], line_h, text, border=1, align="L")
            x += col_w[i]
        pdf.set_xy(x0, y0 + row_h)

    draw_row(cleaned[0], bold=True)
    for r in cleaned[1:]:
        draw_row(r, bold=False)

    pdf.set_font("Helvetica", size=10.5)
    pdf.set_x(pdf.l_margin)
    pdf.ln(3)


def main():
    with open(SRC, encoding="utf-8") as f:
        lines = f.read().splitlines()

    pdf = ReportPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", size=10.5)

    in_code = False
    code_buf = []
    table_buf = []
    in_table = False

    def flush_code():
        nonlocal code_buf
        if code_buf:
            pdf.set_font("Courier", size=8)
            pdf.set_fill_color(240, 240, 240)
            text = "\n".join(code_buf)
            pdf.multi_cell(0, 4, text, fill=True)
            pdf.ln(2)
            pdf.set_font("Helvetica", size=10.5)
            code_buf = []

    def flush_table():
        nonlocal table_buf
        if table_buf:
            rows = []
            for line in table_buf:
                if re.match(r"^\|?\s*-+\s*(\|\s*-+\s*)*\|?$", line):
                    continue
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                rows.append(cells)
            if rows:
                render_table(pdf, rows)
            table_buf = []

    for raw in lines:
        line = raw.rstrip()

        if line.strip().startswith("```"):
            if in_code:
                flush_code()
                in_code = False
            else:
                in_code = True
            continue
        if in_code:
            code_buf.append(line)
            continue

        if line.strip().startswith("|"):
            in_table = True
            table_buf.append(line)
            continue
        elif in_table:
            flush_table()
            in_table = False

        if not line.strip():
            pdf.ln(2)
            continue

        if line.startswith("# "):
            pdf.set_font("Helvetica", "B", 18)
            pdf.multi_cell(0, 10, clean_inline(line[2:]))
            pdf.set_font("Helvetica", size=10.5)
            pdf.ln(1)
        elif line.startswith("## "):
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 14)
            pdf.multi_cell(0, 8, clean_inline(line[3:]))
            pdf.set_font("Helvetica", size=10.5)
        elif line.startswith("### "):
            pdf.ln(1)
            pdf.set_font("Helvetica", "B", 11.5)
            pdf.multi_cell(0, 7, clean_inline(line[4:]))
            pdf.set_font("Helvetica", size=10.5)
        elif line.strip() == "---":
            pdf.ln(1)
            y = pdf.get_y()
            pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
            pdf.ln(3)
        elif line.strip().startswith("- "):
            pdf.set_x(pdf.l_margin + 4)
            pdf.multi_cell(0, 5.5, "- " + clean_inline(line.strip()[2:]))
        elif re.match(r"^\d+\.\s", line.strip()):
            pdf.set_x(pdf.l_margin + 4)
            pdf.multi_cell(0, 5.5, clean_inline(line.strip()))
        else:
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(0, 5.5, clean_inline(line.strip()))

    flush_table()
    flush_code()

    pdf.output(OUT)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
