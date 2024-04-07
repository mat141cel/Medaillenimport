import json
import requests
import xml.etree.ElementTree as ET


def make_dic(titel, link, besitzer, material, diameter, weight, vs_leg, vs_text, img_vs_pfad, rs_leg, rs_text,
             img_rs_pfad, rand_text, literatur, dat_begin, dat_ende, dat_verbal, medailleur_list, dargestellter_list,
             bemerkung,
             lieferant):
    record_dic = {'titel': titel, 'link': link, 'besitzer': besitzer, 'material': material, 'durchmesser': diameter,
                  'gewicht': weight, 'avers': {'leg': vs_leg, 'description': vs_text, 'img_pfad': img_vs_pfad},
                  'revers': {'leg': rs_leg, 'description': rs_text, 'img_pfad': img_rs_pfad}, 'rand_text': rand_text,
                  'literatur': literatur, 'dat_begin': dat_begin, 'dat_ende': dat_ende, 'dat_verbal': dat_verbal,
                  'linked_persons_corporations': {'49': medailleur_list,
                                                  '54': dargestellter_list}, 'bemerkung': bemerkung,
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


def get_oai_urls(metadata_prefix):
    resumption_token = None
    url_list = []
    url = f"https://www.kenom.de/oai/?verb=ListIdentifiers&{metadata_prefix}"

    while True:
        r = make_request(url)
        root = ET.fromstring(r)

        identifiers = root.findall('.//{http://www.openarchives.org/OAI/2.0/}identifier')
        # Extract and print the identifier values
        for identifier in identifiers:
            url_list.append(identifier.text)  # Append each identifier to the list

        # Get the Resumption Token
        resumption_token = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
        # Check for the presence of a resumption token
        if not (resumption_token := resumption_token.text if resumption_token is not None else None):
            break  # Break the loop if there is no resumption token

        # Update the URL for the next request
        url = f"https://www.kenom.de/oai/?verb=ListIdentifiers&resumptionToken={resumption_token}"
        print(url)
        #break # Break the loop for testing purposes

    return url_list



