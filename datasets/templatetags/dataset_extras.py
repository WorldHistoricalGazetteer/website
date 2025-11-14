from django import template
from django.contrib.auth.models import Group
from django.template.defaultfilters import stringfilter
from django.utils.lorem_ipsum import paragraphs
from django.utils.text import capfirst
import ast, json, os, re, textwrap, validators

from main.choices import FEATURE_CLASSES

register = template.Library()

FCLASSES_DICTIONARY = {key: value for key, value in FEATURE_CLASSES}

# test user in group
@register.filter
def has_group(user, group_name):
    group = Group.objects.get(name=group_name)
    return True if group in user.groups.all() else False

@register.filter
def addstr(arg1, arg2):
    """concatenate arg1 & arg2"""
    return str(arg1) + str(arg2)

@register.filter
def basename(value):
    return os.path.basename(os.path.splitext(value)[0])

@register.filter
def cut(value, arg):
    """Removes all values of arg from the given string"""
    return value.replace(arg, '')

@register.simple_tag
def define(val=None):
    return val

@register.filter
def fclasser(val_list):
    """Map feature class codes to their meanings"""
    return '. '.join([FCLASSES_DICTIONARY.get(val, 'Unknown') for val in val_list])

@register.filter
def filename(val):
    nameonly = re.match(R"^.*\/(.*)\.", val)
    return nameonly.group(1)

@register.filter()
def has_group(user, group_name):
    return user.groups.filter(name=group_name).exists()

@register.filter
def haskey(objlist, arg):
    """True if any obj in objlist has key arg"""
    return any(arg in x for x in objlist)

@register.filter
def initcap(value):
    return capfirst(value)

@register.filter
def in_list(value, arg):
    """Returns a boolean of whether the value is in a list"""
    return value in arg.split(',')

@register.simple_tag(takes_context=True)
def is_whg_admin(context):
    request = context['request']
    return request.user.groups.filter(name='whg_admins').exists()

@register.filter
def is_url(val):
    return True if validators.url(val) else False

@register.filter
def join(value,delimit):
    """joins list/array items"""
    if type(value[0]) == int:
        value=map(str,value)
    return delimit.join(value)

@register.filter
def lorem_ipsum(count=1):
    return "\n".join(paragraphs(count, False))

# @register.inclusion_tag('collection_group_create.html')
@register.filter
def nominated(nom):
    return 'checked' if nom == True else 'unchecked'

@register.filter
def parse(obj,key):
    if '/' in key:
        key=key.split('/')
        return obj[key[0]][key[1]]
    else:
        return obj[key]
    """returns value for given key or sub-key"""
    # obj = json.loads(value.replace("'",'"'))
    # return obj[key]

@register.filter
def parsedict(value,key):
    """returns value for given key"""
    #print('parsedict value, key',value,key)
    return value[key]

@register.filter
def parsejson(val,key):
    # my_string = "{'key':'val','key2':2}"
    obj = ast.literal_eval(val)
    return
    if key in obj:
        return obj[key]
    else:
        return 'off'

@register.filter
@register.filter
def parsetest(val, key):
    val = val.strip('"')
    obj = ast.literal_eval(val)
    if key in obj:
        return obj[key]
    else:
        return 'off'

@register.filter
def readmore(txt, numchars):
    dots = '<span id="dots_descrip">...</span>'
    link = '<a href="#" class="a_more_descrip">more</a><span class="more_descrip hidden">'

    if len(txt) <= numchars:
        return txt
    else:
        return txt[:numchars] + dots + link + txt[numchars:] + ' <a href="#" class="ml-2 a_less_descrip hidden">less</a></span>'

@register.filter
def remove(str, tozap):
    return str.replace(tozap, '')

# @register.inclusion_tag('collection_group_create.html')
# @register.filter
@register.simple_tag
def review_check(val=None):
    return 'checked' if val == 'reviewed' else 'unchecked'

@register.filter
def sortts(objlist):
    foo = sorted(objlist, key=lambda x: x['start']['in'])
    return foo

@register.filter
def split(value,delimit):
    """split string to list"""
    # if type(value[0]) == int:
    #     value=map(str,value)
    return value.split(delimit)

@register.filter
def startswith(text, starts):
    if isinstance(text, str):
        return text.startswith(starts)
    return False

@register.filter(name='subtract')
def subtract(value, arg):
    return int((value or 0)) - int((arg or 0))

@register.filter
def time_estimate_sparql(numrows):
    seconds = numrows
    return 'about '+str(round(seconds/60))+' minute(s)' \
           if seconds >= 60 else 'under 1 minute'

@stringfilter
def trimbrackets(value):
    """trims [ and ] from string, returns integer"""
    return int(value[1:-1])

# truncates at previous word break
@register.filter
def trunc_it(str, numchars):
    return textwrap.shorten(str, width=numchars, placeholder="...")

@register.filter
def url_it(val):
    r1 = '((http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?)'
    reg = re.search(r1, val)
    return val.replace(reg.group(1),'<a href="'+reg.group(1)+
        '" target="_blank">link</a>  <i class="fas fa-external-link-alt linky"></i>') if reg else val

@register.filter
def dict_item(dictionary, key):
    return dictionary.get(key)

@register.filter
def dict_key(value, key):
    """Safely get a dictionary value by key."""
    if isinstance(value, dict):
        return value.get(key, '')
    return ''
