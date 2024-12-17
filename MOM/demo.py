import smtplib
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
import base64
from pdf2docx import Converter
pdf_file='1.pdf'
cv=Converter(pdf_file)
cv.convert('1.docx')
cv.close()