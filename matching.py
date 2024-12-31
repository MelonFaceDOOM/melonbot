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
        best_match = ""
        length = self.max + 1
        while length:
            length -= 1
            try:
                 comparator_substrings = comparator.substrings[length]
            except KeyError:
                continue
            for substring in comparator_substrings:
                if substring in self.substrings[length]:
                    match = substring
                    return match
        return None


def find_closest_match(term, bank, threshold=50):
    substr_match = ""
    full_match = ""
    term_substrings = Substrings(term)
    for word in bank:
        word_substrings = Substrings(word)
        match = term_substrings.best_match(word_substrings) # finds the longest contiguous matching substring of the search term and the bank word
        if match:
            if len(match) > len(substr_match):
                substr_match = match
                full_match = word
            if len(match) == len(substr_match):
                substr_match = match
                full_match = word if len(word)<len(full_match) else full_match
    if len(substr_match) < (len(term)*threshold/100):
        return None
    return full_match
    
def find_closest_match_and_score(term, bank, threshold=0.5):
    if len(term) == 0:
        return None, 0
    substr_match = ""
    full_match = ""
    term_substrings = Substrings(term)
    for word in bank:
        word_substrings = Substrings(word)
        match = term_substrings.best_match(word_substrings)
        if match:
            if len(match) > len(substr_match):
                substr_match = match
                full_match = word
            if len(match) == len(substr_match):
                substr_match = match
                full_match = word if len(word)<len(full_match) else full_match
    score = len(substr_match) / len(term) # this uses 0-1 instead of 0-100 which the other func uses
    if score < threshold:
        return None, score
    return full_match, score
    
def rank_matches(term, bank):
    """bank is a list of words and categories
    i.e. [("jacob", "username"), ("jacob's ladder", "movie")]
    this is so that both a user & a movie with the same name can be identified in the search algo"""
    term_substrings = Substrings(term)
    bank_and_score = []
    for word, identifier in bank:
        word_substrings = Substrings(word)
        match = term_substrings.best_match(word_substrings)
        if match:
            score = len(match) / len(term) # 0-1
            score = round(score, 2)
            bank_and_score.append((word, identifier, score))
    if bank_and_score:
        bank_and_score.sort(key=lambda x: x[2], reverse=True)
        return bank_and_score
    else:
        return []
