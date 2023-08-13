import pypdfium2 as pdfium
import io
import requests


def parse_pdf_from_url(url, header_footer_size=65):
    response = requests.get(url=url, stream=True)
    document = pdfium.PdfDocument(io.BytesIO(response.content))

    text_parts = []
    for page in document:
        height = page.get_size()[1]
        textpage = page.get_textpage()
        text_part = textpage.get_text_bounded(
            bottom=header_footer_size, top=height - header_footer_size
        )
        text_parts.append(text_part)
    return text_parts
