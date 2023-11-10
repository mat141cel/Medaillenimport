import json
import requests

def make_dic(titel, link, besitzer, material, diameter, weight, vs_leg, vs_text, img_vs_pfad, rs_leg, rs_text,
             img_rs_pfad, rand_text, literatur, dat_begin, dat_ende, dat_verbal, medailleur_list, dargestellter_list,
             bemerkung,
             lieferant):
    record_dic = {'titel': titel, 'link': link, 'besitzer': besitzer, 'material': material, 'durchmesser': diameter,
                  'gewicht': weight, 'avers': {'leg': vs_leg, 'description': vs_text, 'img_pfad': img_vs_pfad},
                  'revers': {'leg': rs_leg, 'description': rs_text, 'img_pfad': img_rs_pfad}, 'rand_text': rand_text,
                  'literatur': literatur, 'dat_begin': dat_begin, 'dat_ende': dat_ende, 'dat_verbal': dat_verbal,
                  'linked_persons_corporations': {'49': {'ndp_uri': medailleur_list},
                                                  '54': {'ndp_uri': dargestellter_list}}, 'bemerkung': bemerkung,
                  'lieferant': lieferant}
    return record_dic


def make_request(url: str, encoding='utf-8') -> str:
    response = requests.get(url)
    response.encoding = encoding  # Specify the encoding
    return response.text

def extr_text(element, tag, default=None):
    return element.findtext(tag, default=default)

def extract_literal_value(data, key):
    return data.get(key) if data.get(key) else None

def extract_literal_value_from_list(data, key):
    if isinstance(data, list):
        values = [entry.get(key) for entry in data if entry.get(key)]
        return values if values else None
    return None

def save_json(filename, record_list):
    # Generate the JSON string
    json_string = json.dumps(record_list, ensure_ascii=True, indent=4).replace('/', r'\/')

    # Write the JSON string to the file
    with open(f"{filename}.json", "w", encoding='utf-8') as json_file:
        json_file.write(json_string)

def get_object_event_with_tag(object_data, tag):
    for event in object_data.get("object_events", []):
        if event.get("event_type") == tag and event.get("people_id") != 0:
            return event
    return {}




