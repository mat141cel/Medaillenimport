import xml.etree.ElementTree as ET
from datetime import datetime
from reusable_code import *


# Configuration
def main():
    process_oai()
    # get_museum_digital()
    # selection()


def process_oai():
    resumption_token = None
    record_list = []

    while True:
        # get Text from Link
        request_url = "https://www.kenom.de/oai/?verb=ListRecords&metadataPrefix=lido&set=objekttyp:Medaille" + (f'&resumptionToken={resumption_token}' if resumption_token else '')
        response_text = make_request(request_url)
        root = ET.fromstring(response_text)

        # Define namespaces
        l = '{http://www.lido-schema.org}'

        for record in root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
            # simple extraction
            lieferant = "Kenom"
            rand_text = None
            titel = extr_text(record, f".//{l}titleSet/{l}appellationValue")
            link = extr_text(record, f".//{l}objectPublishedID")
            besitzer = extr_text(record, f".//{l}repositoryName/{l}legalBodyName/{l}appellationValue")
            vs_leg = extr_text(record, f".//{l}inscriptionsWrap{l}inscriptions/{l}inscriptionTranscription")
            vs_text = extr_text(record, f".//{l}inscriptions/{l}inscriptionDescription/{l}descriptiveNoteValue")
            img_vs_pfad = extr_text(record, f".//{l}resourceSet/{l}resourceRepresentation/{l}linkResource")
            rs_leg = extr_text(record, f".//{l}inscriptions[2]/{l}inscriptionTranscription")
            rs_text = extr_text(record, f".//{l}inscriptions[2]/{l}inscriptionDescription/{l}descriptiveNoteValue")
            img_rs_pfad = extr_text(record, f".//{l}resourceSet[2]/{l}resourceRepresentation/{l}linkResource")
            dat_begin = extr_text(record, f".//{l}eventDate/{l}date/{l}earliestDate")
            dat_ende = extr_text(record, f".//{l}eventDate/{l}date/{l}latestDate")
            dat_verbal = extr_text(record, f".//{l}eventDate/{l}displayDate")
            bemerkung = extr_text(record, f".//{l}objectDescriptionSet/{l}descriptiveNoteValue")

            # more complicated
            material = (extr_text(record, f".//{l}termMaterialsTech/{l}term") or '').split('>')[-1].strip()

            diameter = weight = None  # Initialize the variables
            for measurement in record.findall(f".//{l}objectMeasurementsSet/{l}objectMeasurements/{l}measurementsSet"):
                measurement_type = extr_text(measurement, f"{l}measurementType")
                measurement_value = extr_text(measurement, f"{l}measurementValue")
                if measurement_type == "diameter":
                    diameter = measurement_value
                elif measurement_type == "weight":
                    weight = measurement_value

            literatur = '\n'.join(
                element.text for element in record.findall(f".//{l}relatedWork/{l}object/{l}objectNote") if
                element.text != "Literatur")

            # Extract medailleur_list using a list comprehension
            medailleur_list = [
                extr_text(person, f"{l}actorInRole/{l}actor/{l}actorID")
                for person in record.findall(f".//{l}eventActor")
                if any(role_keyword in extr_text(person, f"{l}displayActorInRole") for role_keyword in
                       ["Modelleur", "Medailleur", "Hersteller", "Bildhauer", "Künstler", "Entwerfer",
                        "Beteiligte"])
            ]
            # Extract dargestellter_list using a list comprehension
            dargestellter_list = [
                extr_text(dargestellt, f"{l}actor/{l}actorID")
                for dargestellt in record.findall(f".//{l}subjectActor")
                if "Dargestellte Person" in extr_text(dargestellt, f"{l}displayActor")
            ]

            # convert variables to right format
            record_dic = make_dic(titel, link, besitzer, material, diameter, weight, vs_leg, vs_text,
                          img_vs_pfad, rs_leg, rs_text, img_rs_pfad, rand_text, literatur, dat_begin, dat_ende,
                          dat_verbal, medailleur_list, dargestellter_list, bemerkung, lieferant)
            record_list.append(record_dic)

        # Resumption Token
        resumption_token = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
        print(resumption_token)
        if not (resumption_token := resumption_token.text if resumption_token is not None else None):
            break

    save_json("output_kenom", record_list)


def get_museum_digital():
    record_list = []
    # die Liste aller Medaillen laden, die mit der Serie von Huster verknüpft sind:
    response = requests.get("https://nat.museum-digital.de/json/series/1577")
    series_objects = json.loads(response.text).get('series_objects', [])

    # Jeden Link laden
    for object_id in series_objects:
        object_url = f"https://nat.museum-digital.de/json/object/{object_id}"
        response = requests.get(object_url)
        object_data = json.loads(response.text)

        # simple extraction
        vs_leg = rs_leg = None
        lieferant = "Museum-digital"
        titel = extract_literal_value(object_data, 'object_name')
        link = f"https://nat.museum-digital.de/object/{object_id}"
        besitzer = extract_literal_value(object_data.get('object_institution', {}), 'institution_name')
        material = extract_literal_value(object_data, 'object_material_technique')
        bemerkung = extract_literal_value(object_data, 'object_description')
        literatur = extract_literal_value_from_list(object_data.get('object_literature', []), 'literature_name')

        object_images = object_data.get('object_images', [{}])
        img_vs_pfad = object_images[0].get('name')
        img_rs_pfad = object_images[1].get('name') if len(object_images) > 1 else None

        inscr = extract_literal_value(object_data, "inscription")
        vs_text = inscr.split("Vorderseite: ")[-1].split("\nRückseite: ")[0] if inscr else None
        rs_text = inscr.split("\nRückseite: ")[-1].split("\nRand")[0] if inscr else None
        rand_text = inscr.split("\nRand") if inscr else None

        dim = extract_literal_value(object_data, "object_dimensions")
        diameter = dim.split("Durchmesser: ")[-1].split("mm")[0] if dim else None
        height = dim.split("Höhe: ")[-1].split("mm")[0] if dim else None
        width = dim.split("Breite: ")[-1].split("mm")[0] if dim else None
        weight = dim.split("Gewicht: ")[-1].split("g")[0] if dim else None
        if not diameter and height and width:
            diameter = f"{height} x {width}"

        herstellung_event = get_object_event_with_tag(object_data, 1)
        dat_begin = herstellung_event.get("time", {}).get("time_start")
        dat_ende = herstellung_event.get("time", {}).get("time_end")
        dat_verbal = herstellung_event.get("time", {}).get("time_name")

        medailleur_list = f"https://nat.museum-digital.de/people/{extract_literal_value(herstellung_event.get('people', [{}]), 'people_id')}" if herstellung_event else None

        dargestellter_event = get_object_event_with_tag(object_data, 5)
        dargestellter_list = []
        if extract_literal_value(dargestellter_event, 'people_id') is not None:
            dargestellter_list = f"https://nat.museum-digital.de/people/{extract_literal_value(dargestellter_event, 'people_id')}"

        # convert variables to right format
        record_dic = make_dic(titel, link, besitzer, material, diameter, weight, vs_leg, vs_text,
                              img_vs_pfad, rs_leg, rs_text, img_rs_pfad, rand_text, literatur, dat_begin, dat_ende,
                              dat_verbal, medailleur_list, dargestellter_list, bemerkung, lieferant)
        record_list.append(record_dic)

    save_json("output_md", record_list)


def selection():
    # Read the JSON data from "output_kenom.json"
    with open('output_kenom.json', 'r') as file:
        kenom_data = json.load(file)

    # Define the filter criteria
    target_besitzer = "Kulturstiftung Sachsen-Anhalt, Kunstmuseum Moritzburg Halle (Saale)"
    target_dat_ende = datetime(1871, 1, 1)

    # Read the "ndp_gbv.json" data
    with open('NDP_GBV.json', 'r') as file:
        ndp_gbv_data = json.load(file)

    # Define a function to convert dat_ende to a datetime object or None
    def parse_dat_ende(dat_ende):
        if isinstance(dat_ende, int):
            return datetime(dat_ende, 1, 1)
        try:
            return datetime.strptime(dat_ende, '%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    # Filter and update the data in one go
    filtered_data = []

    for record in kenom_data:
        dat_ende = parse_dat_ende(record.get("dat_ende"))
        if (
            record.get("besitzer") == target_besitzer and
            (dat_ende is not None and dat_ende >= target_dat_ende)
        ):
            updated_record = {
                **record,
                'linked_persons_corporations': {
                    key: {
                        "ndp_uri": [
                            entry['GBV']
                            for entry in ndp_gbv_data
                            if entry['NDP'] in (value.get("ndp_uri", []) if isinstance(value, dict) else [])
                        ]
                    } if isinstance(value, dict) and "ndp_uri" in value else value
                    for key, value in record.get("linked_persons_corporations", {}).items()
                }
            }
            filtered_data.append(updated_record)

    # Save the updated data to a new JSON file
    with open('updated_kenom.json', 'w') as output_file:
        json.dump(filtered_data, output_file, indent=2)


main()