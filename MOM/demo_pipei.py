import difflib

def fuzzy_match(s1, s2):
    return difflib.SequenceMatcher(None, s1, s2).ratio()

s1 = "衍复希格斯三号"
s2 = "衍复希格斯一号"
q=min(len(s1),len(s2))
print(q)
similarity = fuzzy_match(s1[0:q], s2[0:q])
print(similarity)
if similarity>0.80:
    print(f"Similarity: {similarity}")