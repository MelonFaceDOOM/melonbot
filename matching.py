class Substrings:
    def __init__(self, term):
        self.name = term.lower()
        self.substrings = self.get_substrings()
        if self.substrings:
            self.max = max(self.substrings.keys())
        else:
            self.max = 0
    
    def get_substrings(self):
        substrings = {}
        frame_length = 1
        while frame_length <= len(self.name):
            n_frames = len(self.name) - frame_length + 1
            frame_substrings = []
            for i in range(n_frames):
                frame_substrings.append(self.name[i:i+frame_length])
            substrings[frame_length] = frame_substrings
            frame_length += 1
        return substrings
    
    def best_match(self, comparator):
        # finds the longest contiguous matching substring of the this substring and a 2nd substring
        length = self.max + 1
        while length:
            length -= 1
            try:
                 comparator_substrings = comparator.substrings[length]
            except KeyError:
                continue
            for substring in comparator_substrings:
                if substring in self.substrings[length]:
                    return substring
        return ""


# def find_closest_match(search_term, bank, threshold=50):
    # substr_match = ""
    # full_match = ""
    # search_term_substrings = Substrings(search_term)
    # for word in bank:
        # word_substrings = Substrings(word)
        # match = search_term_substrings.best_match(word_substrings) # finds the longest contiguous matching substring of the search term and the bank word
        # if match:
            # if len(match) > len(substr_match):
                # substr_match = match
                # full_match = word
            # if len(match) == len(substr_match):
                # substr_match = match
                # full_match = word if len(word)<len(full_match) else full_match
    # if len(substr_match) < (len(search_term)*threshold/100):
        # return None
    # return full_match
    
def find_closest_match_and_score(search_term, bank, threshold=0.5):
    if len(search_term) == 0:
        return None, 0
    substr_match = ""
    full_match = ""
    bank_and_score = []
    for bank_item in bank:
        match, score = find_best_match(search_term, bank_item)
        if score > threshold:
            bank_and_score.append((bank_item, score))
    if bank_and_score:
        bank_and_score.sort(key=lambda x: x[1], reverse=True)
        return bank_and_score[0]
    return None, None
    
def rank_matches(search_term, bank):
    """bank is a list of words, identifiers, and scores
    i.e. [("jacob", "username", 1), ("jacob's ladder", "movie", 0.8)]
    this is so that both a user & a movie with the same name can be identified in the search algo"""
    bank_and_score = []
    for bank_item, identifier in bank:
        match, score = find_best_match(search_term, bank_item)
        bank_and_score.append((bank_item, identifier, score))
    if bank_and_score:
        bank_and_score.sort(key=lambda x: x[2], reverse=True)
        return bank_and_score
    else:
        return []
    
def find_best_match(search_term, comparator):
    search_term_substrings = Substrings(search_term)
    comparator_substrings = Substrings(comparator)
    best_match = search_term_substrings.best_match(comparator_substrings)
    search_term_extra_letter_penalty = len(best_match) / len(search_term) # what % of search term chars are extra
    comparator_extra_letter_penalty = len(best_match) / len(comparator) # what % of chars from the word bank item that is extra
    score = search_term_extra_letter_penalty * comparator_extra_letter_penalty
    score = round(score, 2)
    return best_match, score
