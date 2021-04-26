
# an old function for parsing the numbers in the react

def get_react_num(raw,top_post=False):
    def parseSize(size): #parse size code edited from here: https://stackoverflow.com/questions/42865724/python-parse-human-readable-filesizes-into-bytes
        units = {"B": 1, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12}
        unit = size[-1]
        number = size[:-1]
        return int(float(number)*units[unit])
    try:
        react = int(raw)
    except ValueError:
        try:
            react = [int(s) for s in raw.split() if s.isdigit()][0]
            if top_post:
                react += 1
        except IndexError:
            try:
                react = parseSize(raw)
                print("**FOUND {r} reactions **".format(r=react))
            except:
                print(raw)
                raise
    return react