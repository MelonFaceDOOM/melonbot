import requests
import re
from lxml import html
from scraping.google import search

def ebert_lookup(movie):
    url = ""
    title = "[failed to extract title]"
    author = "[failed to extract author]"
    star_count = "[failed to extract rating]"
    first_paragraph = "[failed to extract first paragraph]"
    try:
        url = search(f"{movie} site:https://www.rogerebert.com/")['items'][0]['link']
    except:
        return "Google search failed."

    try:
        page = requests.get(url)
    except:
        return f"Found the following url but failed to retrieve it: {url}"
    tree = html.fromstring(page.content)
    
    title_element = tree.xpath('//h1[contains(@class,"page-title")]')
    if title_element:
        title = title_element[0].text.upper()
    author_element = tree.xpath('//a[contains(@href, "https://www.rogerebert.com/contributors/")]/text()')
    if author_element:
        author = author_element[0]
    star_element = tree.xpath('//div[@class="star-box"]/img[contains(@class, "h-7 filled star")]')
    if star_element:
        star_count = extract_star_rating_from_star_element(star_element[0])

    first_paragraph_element = tree.xpath('//div[contains(@class, "entry-content text")]/p')
    if first_paragraph_element:
        first_paragraph = first_paragraph_element[0].text_content().strip()
        first_paragraph.replace("\'", "")
    
    message = f'{title} - {star_count}/4\n- by {author}\n'
    message += first_paragraph
    message += "\n read full review: " + url

    return message
    
def extract_star_rating_from_star_element(star_element):
    class_val = star_element.get('class')  # e.g., "h-7 filled star35"
    match = re.search(r'star(\d+)', class_val)
    if match:
        number_str = match.group(1)  # e.g., "35"
        rating = str(float(number_str) / 10) # 35 becomes 3.5, 40 becomes 4.0
        return rating
    return None