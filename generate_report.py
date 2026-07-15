"""Generate a formal Vendor Performance Analysis report as a PDF."""
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER, TA_LEFT
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
                                HRFlowable, ListFlowable, ListItem, PageBreak)

# ---- Brand palette (matches the dashboard's teal theme) ----
TEAL = colors.HexColor("#2F6B6B")
DARK = colors.HexColor("#1F2A2A")
LIGHT = colors.HexColor("#EAF1F1")
GREY = colors.HexColor("#5A6666")
ACCENT = colors.HexColor("#C89B3C")

styles = getSampleStyleSheet()

def S(name, **kw):
    return ParagraphStyle(name, parent=styles["Normal"], **kw)

title_style   = S("t", fontName="Helvetica-Bold", fontSize=26, textColor=TEAL, leading=30, spaceAfter=4)
subtitle_style= S("st", fontName="Helvetica", fontSize=12, textColor=GREY, leading=16, spaceAfter=2)
h1            = S("h1", fontName="Helvetica-Bold", fontSize=15, textColor=TEAL, leading=19, spaceBefore=16, spaceAfter=6)
h2            = S("h2", fontName="Helvetica-Bold", fontSize=12, textColor=DARK, leading=15, spaceBefore=8, spaceAfter=3)
body          = S("b", fontName="Helvetica", fontSize=10.5, textColor=DARK, leading=15, alignment=TA_JUSTIFY, spaceAfter=6)
bullet        = S("bu", fontName="Helvetica", fontSize=10.5, textColor=DARK, leading=15)
small         = S("sm", fontName="Helvetica-Oblique", fontSize=9, textColor=GREY, leading=12)
kpi_num       = S("kn", fontName="Helvetica-Bold", fontSize=16, textColor=TEAL, alignment=TA_CENTER, leading=18)
kpi_lbl       = S("kl", fontName="Helvetica", fontSize=8.5, textColor=GREY, alignment=TA_CENTER, leading=11)

story = []

def rule(color=TEAL, w=1.2, space=8):
    story.append(Spacer(1, 4))
    story.append(HRFlowable(width="100%", thickness=w, color=color, spaceAfter=space))

def bullets(items):
    lf = ListFlowable(
        [ListItem(Paragraph(t, bullet), leftIndent=6, value="•") for t in items],
        bulletType="bullet", start="•", leftIndent=14, bulletColor=TEAL,
    )
    story.append(lf)
    story.append(Spacer(1, 6))

# ===================== HEADER =====================
story.append(Paragraph("Vendor Performance Analysis", title_style))
story.append(Paragraph("An End-to-End Data Analytics Case Study &mdash; SQL, Python &amp; Power BI", subtitle_style))
story.append(Paragraph("Retail &amp; Wholesale Distribution &nbsp;|&nbsp; Full-Year Transaction Data", small))
rule()

# KPI strip
kpi_data = [[
    Paragraph("$451.6M", kpi_num), Paragraph("$321.9M", kpi_num),
    Paragraph("$129.7M", kpi_num), Paragraph("126", kpi_num), Paragraph("$15.6M", kpi_num),
],[
    Paragraph("Total Sales", kpi_lbl), Paragraph("Total Purchases", kpi_lbl),
    Paragraph("Gross Profit", kpi_lbl), Paragraph("Vendors", kpi_lbl), Paragraph("Unsold Capital", kpi_lbl),
]]
kpi_tbl = Table(kpi_data, colWidths=[3.3*cm]*5)
kpi_tbl.setStyle(TableStyle([
    ("BACKGROUND",(0,0),(-1,-1), LIGHT),
    ("BOX",(0,0),(-1,-1),0.5,colors.white),
    ("INNERGRID",(0,0),(-1,-1),3,colors.white),
    ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ("TOPPADDING",(0,0),(-1,0),8), ("BOTTOMPADDING",(0,1),(-1,1),8),
]))
story.append(kpi_tbl)
story.append(Spacer(1, 10))

# ===================== EXECUTIVE SUMMARY =====================
story.append(Paragraph("Executive Summary", h1))
story.append(Paragraph(
    "This analysis examines a full year of purchasing and sales data across 126 vendors and 10,692 "
    "vendor&ndash;brand combinations to identify opportunities for improving profitability and inventory "
    "efficiency. The business generated <b>$451.6M in sales</b> against <b>$321.9M in purchases</b>, "
    "yielding <b>$129.7M in gross profit</b>. However, the analysis surfaces several actionable issues: "
    "a heavy dependence on a small number of vendors, <b>$15.6M of capital frozen in unsold inventory</b>, "
    "and a large set of high-margin products that are significantly under-sold. Statistical testing "
    "confirms a genuine, structural difference between high-volume and low-volume vendors&mdash;pointing to "
    "two distinct business models that warrant different strategies.", body))

# ===================== BUSINESS PROBLEM =====================
story.append(Paragraph("1. Business Problem &amp; Objectives", h1))
story.append(Paragraph(
    "Effective inventory and sales management are critical to profitability in the retail and wholesale "
    "industry. Inefficient pricing, poor inventory turnover, or vendor dependency can erode margins. "
    "This analysis was commissioned to:", body))
bullets([
    "Identify underperforming brands that require promotional or pricing adjustments.",
    "Determine the top vendors contributing to sales and gross profit.",
    "Analyse the impact of bulk purchasing on unit costs.",
    "Assess inventory turnover to reduce holding costs.",
    "Investigate the profitability variance between high- and low-performing vendors.",
])

# ===================== DATA & METHODOLOGY =====================
story.append(Paragraph("2. Data &amp; Methodology", h1))
story.append(Paragraph(
    "Six raw transactional datasets (purchases, sales, vendor invoices, pricing, and opening/closing "
    "inventory&mdash;over 15 million rows in total) were ingested into a SQLite database. Using SQL, the "
    "tables were aggregated and joined into a single analytical table (<i>vendor_sales_summary</i>) at the "
    "vendor&ndash;brand grain, with derived metrics including gross profit, profit margin, stock turnover, "
    "and sales-to-purchase ratio. The data was cleaned in Python (type correction, whitespace removal, "
    "handling of never-sold items), explored through EDA, and validated statistically. Results were "
    "presented in an interactive Power BI dashboard.", body))
story.append(Paragraph("Tools: SQL (SQLite) &bull; Python (pandas, matplotlib, seaborn, SciPy) &bull; Power BI", small))

story.append(PageBreak())

# ===================== KEY FINDINGS =====================
story.append(Paragraph("3. Key Findings", h1))

story.append(Paragraph("3.1&nbsp;&nbsp;Underperforming brands represent a clear promotion opportunity", h2))
story.append(Paragraph(
    "<b>198 brands</b> exhibit high profit margins (&ge;65%) but very low sales (&le;$560 per year). "
    "These products are highly profitable per unit yet under-exposed&mdash;for example, Crown Royal Apple "
    "earns an ~90% margin on only ~$28 of annual sales. Growing their volume would add profit with minimal "
    "margin risk.", body))

story.append(Paragraph("3.2&nbsp;&nbsp;Purchasing is heavily concentrated among a few vendors", h2))
story.append(Paragraph(
    "The <b>top 10 of 126 vendors account for 65.7% of total purchase spend</b>, with Diageo North America "
    "alone representing <b>16.3%</b>. This concentration creates supply-chain dependency risk: disruption "
    "or price changes from a single major vendor would materially affect operations.", body))

story.append(Paragraph("3.3&nbsp;&nbsp;Bulk purchasing materially reduces unit cost", h2))
story.append(Paragraph(
    "Grouping purchases into small, medium, and large order sizes shows that <b>large orders achieve a "
    "median unit cost of ~$8 versus ~$13 for small orders&mdash;a 35&ndash;40% saving</b>. Larger orders "
    "also show more consistent pricing, confirming clear economies of scale.", body))

story.append(Paragraph("3.4&nbsp;&nbsp;$15.6M of capital is locked in unsold inventory", h2))
story.append(Paragraph(
    "An estimated <b>$15.6M is tied up in stock that was purchased but not sold</b>, including 178 brands "
    "with zero sales. The unsold capital is concentrated among large vendors&mdash;Martignetti Companies "
    "($1.9M), Diageo ($1.7M), and Ultra Beverage ($1.5M)&mdash;indicating over-purchasing relative to "
    "sales velocity.", body))

story.append(Paragraph("3.5&nbsp;&nbsp;Low-volume vendors are significantly more profitable per dollar", h2))
story.append(Paragraph(
    "A comparison of the top 25% and bottom 25% of vendors by sales reveals that low-volume vendors earn "
    "markedly higher margins. This was validated with a Welch&rsquo;s two-sample t-test:", body))

# stats table
stat_rows = [
    ["Vendor group", "Mean profit margin", "95% confidence interval"],
    ["High-volume (top 25%)", "31.2%", "(30.7%, 31.6%)"],
    ["Low-volume (bottom 25%)", "41.6%", "(40.5%, 42.6%)"],
]
stat_tbl = Table(stat_rows, colWidths=[6*cm, 4.5*cm, 5.5*cm])
stat_tbl.setStyle(TableStyle([
    ("BACKGROUND",(0,0),(-1,0), TEAL),
    ("TEXTCOLOR",(0,0),(-1,0), colors.white),
    ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
    ("FONTNAME",(0,1),(-1,-1),"Helvetica"),
    ("FONTSIZE",(0,0),(-1,-1),10),
    ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white, LIGHT]),
    ("GRID",(0,0),(-1,-1),0.5,colors.white),
    ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6),
    ("LEFTPADDING",(0,0),(-1,-1),8),
]))
story.append(stat_tbl)
story.append(Spacer(1, 6))
story.append(Paragraph(
    "The test returned <b>t = &minus;17.67, p &lt; 0.001</b>, and the confidence intervals do not overlap. "
    "We therefore reject the null hypothesis of equal margins: the difference is statistically significant. "
    "High-volume vendors compete on price at thinner margins, while low-volume vendors operate a "
    "premium/niche model at higher margins.", body))

story.append(PageBreak())

# ===================== RECOMMENDATIONS =====================
story.append(Paragraph("4. Recommendations", h1))
bullets([
    "<b>Promote high-margin, low-sales brands.</b> Target the 198 identified products with marketing, "
    "improved shelf placement, or bundling to grow volume with minimal margin risk.",
    "<b>Mitigate vendor-dependency risk.</b> Secure favourable contracts with the top vendors and develop "
    "backup suppliers for the top 2&ndash;3 to reduce single-source exposure.",
    "<b>Consolidate purchasing to capture bulk discounts.</b> Combine fragmented small orders into larger "
    "ones where demand supports it, targeting the 35&ndash;40% unit-cost saving.",
    "<b>Release frozen capital.</b> Review reorder quantities for over-stocked vendors, run clearance on "
    "slow-moving lines, and align purchase volumes to actual sales velocity to free up the $15.6M.",
    "<b>Differentiate vendor strategy.</b> Manage high-volume vendors for total profit and protect "
    "low-volume, high-margin vendors as strategic contributors&mdash;do not treat low sales as low value.",
])

# ===================== CONCLUSION =====================
story.append(Paragraph("5. Conclusion", h1))
story.append(Paragraph(
    "The analysis translates raw transactional data into a coherent set of profitability and inventory "
    "insights. The central theme is a trade-off between <b>volume and margin</b>: the business earns most "
    "of its total profit from a few high-volume, thin-margin vendors, while its highest margins sit with "
    "small, under-promoted products and niche vendors. Acting on the five recommendations&mdash;promoting "
    "hidden-gem brands, diversifying vendors, consolidating orders, releasing dead capital, and tailoring "
    "vendor strategy&mdash;would improve both profitability and capital efficiency.", body))

rule(color=GREY, w=0.6, space=4)
story.append(Paragraph(
    "Prepared as an end-to-end data analytics case study. Figures derived from the vendor_sales_summary "
    "dataset; statistical testing performed at the 95% confidence level.", small))

# ---- build ----
doc = SimpleDocTemplate(
    "Vendor_Performance_Report.pdf", pagesize=A4,
    leftMargin=2*cm, rightMargin=2*cm, topMargin=1.8*cm, bottomMargin=1.8*cm,
    title="Vendor Performance Analysis Report", author="Data Analytics",
)
doc.build(story)
print("Created Vendor_Performance_Report.pdf")
