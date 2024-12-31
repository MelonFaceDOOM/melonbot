import requests
from lxml import html
from scraping.google import search
import re
import random


def find_rt_page(movie):
    # hope and pray that the main rt page is the #1 result
    try:
        top_result_url = search(f'{movie} site:https://www.rottentomatoes.com/')['items'][0]['link']
    except:
        raise ValueError('Google search failed to find anything')
    top_result_url += '/reviews'
    return top_result_url


def random_tomato(movie, fresh=2):
    """fresh=0 for rotten, 1 for fresh, 2 for either"""
    reviews_url = find_rt_page(movie)
    page = requests.get(reviews_url)
    tree = html.fromstring(page.content)
    page_nav = tree.xpath('//span[@class="pageInfo"]')
    if page_nav:
        page_nav = page_nav[0].text
        pattern = " ([0-9]+)$"
        n_pages = re.search(pattern, page_nav).groups()[0]
        page_nums = list(range(1, int(n_pages) + 1))
        random.shuffle(page_nums)
    else:
        page_nums = [1]

    suitable_review = None
    while suitable_review is None:
        for page_num in page_nums:
            page = requests.get(reviews_url + f'?type=&sort=&page={page_num}')
            tree = html.fromstring(page.content)
            reviews = tree.xpath('//div[@class="row review_table_row"]')
            random.shuffle(reviews)
            for review in reviews:
                potential_review = mine_review(review)
                if fresh == 0:
                    if potential_review['tomato'] == "rotten":
                        suitable_review = potential_review
                        break
                elif fresh == 1:
                    if potential_review['tomato'] == "fresh":
                        suitable_review = potential_review
                        break
                elif fresh == 2:
                    suitable_review = potential_review
                else:
                    raise ValueError('argument "fresh" must be 0, 1, or 2')
            if suitable_review:
                break
        break

    if suitable_review is None:
        message = "No suitable review could be found"
    else:
        message = f'------ {movie.upper()} ------\n'
        message += f'- {suitable_review["author"]}, {suitable_review["publication"]}\n'
        message += f'{suitable_review["original_score"]}\n'
        message += f'{suitable_review["text"]}\n\n'
        if suitable_review['link']:
            message += f'read full review: {suitable_review["link"]}'
    message += f'\ncheck out more reviews at: {reviews_url}'
    return message


def mine_review(review_tree):
    review = {}
    fresh = review_tree.xpath('.//div[@class="review_icon icon small fresh"]')
    rotten = review_tree.xpath('.//div[@class="review_icon icon small rotten"]')
    if fresh:
        review['tomato'] = 'fresh'
    elif rotten:
        review['tomato'] = 'rotten'
    else:
        review['tomato'] = 'unknown'

    name = review_tree.xpath('.//div[contains(@class, "critic_name")]/a')
    if name:
        review['author'] = name[0].text
    else:
        review['author'] = ""

    publication = review_tree.xpath('.//em[contains(@class, "critic-publication")]')
    if publication:
        review['publication'] = publication[0].text
    else:
        review['publication'] = ""

    text = review_tree.xpath('.//div[@class="the_review"]')
    if text:
        review['text'] = text[0].text.strip()
    else:
        review['text'] = ""

    link = review_tree.xpath('.//div[contains(@class, "review-link")]/a')
    if link:
        review['link'] = link[0].attrib['href']
    else:
        review['link'] = ""

    original_score = review_tree.xpath('.//div[contains(@class, "review-link")]/a')
    if original_score:
        review['original_score'] = original_score[0].tail.strip('| \n')
    else:
        review['original_score'] = ""
    return review