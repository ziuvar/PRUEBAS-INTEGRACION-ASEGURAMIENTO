from __future__ import annotations

import html
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, Preformatted, SimpleDocTemplate, Spacer


def build_story(markdown_text: str):
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleCustom",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#1f2937"),
        spaceAfter=12,
    )
    h1_style = ParagraphStyle(
        "Heading1Custom",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#111827"),
        spaceBefore=8,
        spaceAfter=6,
    )
    h2_style = ParagraphStyle(
        "Heading2Custom",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=16,
        textColor=colors.HexColor("#111827"),
        spaceBefore=6,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "BodyCustom",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        spaceAfter=4,
    )
    code_style = ParagraphStyle(
        "CodeCustom",
        parent=styles["Code"],
        fontName="Courier",
        fontSize=8.5,
        leading=11,
        leftIndent=14,
        rightIndent=14,
        spaceBefore=4,
        spaceAfter=6,
    )

    story = []
    in_code = False
    code_lines: list[str] = []

    for raw_line in markdown_text.splitlines():
        line = raw_line.rstrip()

        if line.startswith("```"):
            if in_code:
                story.append(Preformatted("\n".join(code_lines), code_style))
                story.append(Spacer(1, 0.05 * inch))
                code_lines = []
                in_code = False
            else:
                in_code = True
            continue

        if in_code:
            code_lines.append(line)
            continue

        if not line.strip():
            story.append(Spacer(1, 0.08 * inch))
            continue

        escaped = html.escape(line)

        if line.startswith("# "):
            story.append(Paragraph(html.escape(line[2:]), title_style))
        elif line.startswith("## "):
            story.append(Paragraph(html.escape(line[3:]), h1_style))
        elif line.startswith("### "):
            story.append(Paragraph(html.escape(line[4:]), h2_style))
        elif line.startswith("#### "):
            story.append(Paragraph(html.escape(line[5:]), body_style))
        else:
            escaped = escaped.replace("`", "")
            story.append(Paragraph(escaped, body_style))

    if code_lines:
        story.append(Preformatted("\n".join(code_lines), code_style))

    return story


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    source = repo_root / "docs" / "INFORME_PRUEBAS_INTEGRACION.md"
    target = repo_root / "docs" / "INFORME_PRUEBAS_INTEGRACION.pdf"

    story = build_story(source.read_text(encoding="utf-8"))
    doc = SimpleDocTemplate(
        str(target),
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
        title="Informe de Pruebas de Integracion",
        author="Codex",
    )
    doc.build(story)
    print(target)


if __name__ == "__main__":
    main()
