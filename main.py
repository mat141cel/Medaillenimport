from reusable_code import *
import asyncio
import re


# Configuration
def main():
    #asyncio.run(process_oai())
    get_museum_digital()
    #selection(49, "kenom")
    selection(49, "md")
    #send_to_API("kenom")
    send_to_API("md")


async def fetch_url(url, record_list):
    response_text = make_request(url)
    root = ET.fromstring(response_text)

    # Define namespaces
    l = '{http://www.lido-schema.org}'
    for record in root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
        # simple extraction
        lieferant = "Kenom"
        rand_text = None
        titel = extr_text(record, f".//{l}titleSet[2]/{l}appellationValue")
        link = extr_text(record, f".//{l}objectPublishedID")
        besitzer = extr_text(record, f".//{l}repositoryName/{l}legalBodyName/{l}appellationValue")
        vs_leg = extr_text(record, f".//{l}inscriptionsWrap{l}inscriptions/{l}inscriptionTranscription")
        vs_text = extr_text(record, f".//{l}inscriptions/{l}inscriptionDescription/{l}descriptiveNoteValue")
        img_vs_pfad = extr_text(record, f".//{l}resourceSet/{l}resourceRepresentation/{l}linkResource")
        rs_leg = extr_text(record, f".//{l}inscriptions[2]/{l}inscriptionTranscription")
        rs_text = extr_text(record, f".//{l}inscriptions[2]/{l}inscriptionDescription/{l}descriptiveNoteValue")
        img_rs_pfad = extr_text(record, f".//{l}resourceSet[2]/{l}resourceRepresentation/{l}linkResource")
        dat_begin = extr_text(record, f".//{l}event[{l}eventType/{l}term='Herstellung']/{l}eventDate/{l}date/{l}earliestDate")
        dat_ende = extr_text(record, f".//{l}event[{l}eventType/{l}term='Herstellung']/{l}eventDate/{l}date/{l}latestDate")
        dat_verbal = extr_text(record, f".//{l}event[{l}eventType/{l}term='Herstellung']/{l}eventDate/{l}date/{l}displayDate")
        bemerkung = extr_text(record, f".//{l}objectDescriptionSet/{l}descriptiveNoteValue")

        # more complicated
        material = (extr_text(record, f".//{l}termMaterialsTech/{l}term") or '').split('>')[-1].strip()

        diameter = weight = None  # Initialize the variables
        for measurement in record.findall(f".//{l}objectMeasurementsSet/{l}objectMeasurements/{l}measurementsSet"):
            measurement_type = extr_text(measurement, f"{l}measurementType")
            measurement_value = extr_text(measurement, f"{l}measurementValue")
            if measurement_type in ("diameter", "height"):
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


async def process_oai():
    object_list = get_oai_urls("metadataPrefix=oai_dc&set=institution:DE-MUS-805518")
    institution_list = get_oai_urls("metadataPrefix=oai_dc&set=objekttyp:Medaille")
    url_list = []
    for identifier in object_list:
        if identifier in institution_list:
            url_list.append(identifier)
    print(F"number of medals to scrape {len(url_list)}")

    # Set the desired rate of requests per second
    requests_per_second = 20
    record_list = []
    async def process_url(url):
        await fetch_url(f"https://www.kenom.de/oai/?verb=GetRecord&metadataPrefix=lido&identifier={url}", record_list)
    tasks = [process_url(url) for url in url_list]
    await asyncio.gather(*tasks)

    save_json("output/output_kenom", record_list)


def get_museum_digital():
    record_list = []
    # die Liste aller Medaillen laden, die mit der Serie von Huster verknüpft sind:
    response = requests.get("https://nat.museum-digital.de/json/series/1577")
    series_objects = json.loads(response.text).get('series_objects', [])
    print("MD medal list loaded")
    series_objects = (
        35965,
        35966
    )

    # Jeden Link laden
    for object_id in series_objects:
        print(object_id, len(series_objects))
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

        def extract_section_text(inscr, section):
            matches = re.findall(rf'({section}\s*:\s*(.*?)\s*(?=\n|$))', inscr, re.DOTALL) if inscr else None
            return '\n'.join(match[0] for match in matches) if inscr else None

        vs_text = extract_section_text(inscr, "Vorderseite")
        rs_text = extract_section_text(inscr, "Rückseite")
        rand_text = extract_section_text(inscr, "Rand")

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

    save_json("output/output_md", record_list)


def selection(number, institution):
    """
    replaces the urls of the institutions with the corresponding NDP
    :param number: either 49 for medailleur or 52 for dargestellter
    :param institution: either kenom or md
    :return: creates a json with the new values
    """

    # Read the JSON data from "output_kenom.json"
    with open(f"output/output_{institution}.json", 'r') as file:
        data = json.load(file)

    # Read the "ndp_gbv.json" data
    with open(f"mapping/{institution}-{number}.json", 'r') as file:
        mapping = json.load(file)

    new_data = []
    log_data = []

    print(f"{institution} out of {len(data)} values ", end="")  # number of values before mapping
    for i, record in enumerate(data):
        value = record.get('linked_persons_corporations', {}).get('49', '')
        replacement = [
            entry['NDP']
            for entry in mapping
            if entry.get('other') == (value if isinstance(value, str) else None)
               or entry.get('other') in (value if isinstance(value, list) else [])
        ]
        if replacement:
            record['linked_persons_corporations']['49'] = replacement
            new_data.append(record)
        else:
            log_data.append([record['link'], record['linked_persons_corporations']['49']])

        # hier eigentlich zwei unterschiedliche Logs erstellen, wenn die Medailleure noch nicht drin sind
        # und wenn die Medailleure zu alt für medaillenkunst sind
    print(f"selected {len(new_data)}")  # number after

    # Save the updated data to a new JSON file
    with open(f'output/{institution}_mapped.json', 'w') as output_f:
        json.dump(new_data, output_f, indent=2)

    with open(f"logs/{institution}_mapping_log.json", 'w') as log_f:
        json.dump(log_data, log_f, indent=2)


def send_to_API(institution):
    with open(f"output/{institution}_mapped.json", 'r') as institution_f:
        data = json.load(institution_f)

    # API endpoint
    api_url = "https://medaillenkunst.de/api-medal.v1.php"

    # Open the log file in append mode
    with open(f"logs/{institution}api_log.txt", "a") as log_file:
        for record in data:
            # Prepare the payload for the POST request
            payload = {"medal": json.dumps(record)}
            # Send POST request to the API
            response = requests.post(api_url, data=payload)

            # Write to the log file with newline characters
            log_file.write(f"{record['link']}, response: {response.status_code}: {response.text}\n")

            # Optionally, you can also print the information to the console
            print(f"Status Code: {record['link']} {response.status_code}")
            print("Response Message:", response.text)

main()
