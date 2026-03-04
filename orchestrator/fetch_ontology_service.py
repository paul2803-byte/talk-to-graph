import requests
import yaml
from models.ontology import Ontology, OntologyObject, Attribute

class FetchOntologyService:
    """
    Service to fetch ontology content from a URL in YAML format and return an Ontology object.
    """

    def fetch_ontology(self, url: str) -> Ontology:
        """
        Calls the provided URL (appending /yaml), fetches the content, 
        validates it is valid YAML, and returns an Ontology structure.

        Args:
            url (str): The URL to fetch the ontology from.

        Returns:
            Ontology: The structured ontology object.

        Raises:
            ValueError: If the content is not valid YAML or missing required keys.
            requests.exceptions.RequestException: If the URL call fails.
        """
        # Append /yaml to the URL as requested
        yaml_url = url.rstrip('/') + '/yaml'
        
        try:
            response = requests.get(yaml_url, timeout=30)
            response.raise_for_status()
            
            try:
                # Use safe_load for security
                data = yaml.safe_load(response.text)
                
                if not data or 'content' not in data or 'meta' not in data:
                    raise ValueError("Content or meta section missing in ontology YAML")

                meta_name = data.get('meta', {}).get('name', 'Anonymisation')
                base_uri = f"https://soya.ownyourdata.eu/{meta_name}/"
                
                ontology = Ontology(
                    prefix="oyd",
                    base_uri=base_uri,
                    objects=[]
                )

                for base in data.get('content', {}).get('bases', []):
                    name = base.get('name')
                    if not name:
                        continue
                        
                    obj = OntologyObject(name=name, attributes=[])

                    # Find matching OverlayClassification overlay for this base
                    overlay_attrs = {}
                    for overlay in data.get('content', {}).get('overlays', []):
                        if overlay.get('base') == name and overlay.get('type') == 'OverlayClassification':
                            overlay_attrs = overlay.get('attributes', {})
                            break
                    
                    attributes = base.get('attributes', {})

                    for attr_name, attr_values in attributes.items():
                        overlay_values = overlay_attrs.get(attr_name, [])
                        if isinstance(overlay_values, list) and len(overlay_values) >= 2:
                            anonymization_type = overlay_values[0]
                            sensitivity_level = overlay_values[1]
                        else:
                            anonymization_type = ''
                            sensitivity_level = 'sensitive'
                        obj.attributes.append(Attribute(
                            name=attr_name,
                            anonymization_type=anonymization_type,
                            sensitivity_level=sensitivity_level
                        ))
                    
                    ontology.objects.append(obj)
                
                return ontology
                
            except yaml.YAMLError as e:
                raise ValueError(f"Content from URL is not valid YAML: {str(e)}")
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch ontology from {yaml_url}: {str(e)}")

if __name__ == "__main__":
    # Quick manual test block
    service = FetchOntologyService()
