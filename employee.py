import requests
import json


def createCore(p_core_name):
    url = f'http://localhost:8983/solr/admin/cores?action=CREATE&name={p_core_name}&instanceDir={p_core_name}&configSet=_default'
    response = requests.get(url)
    if response.ok:
        print(f"Core '{p_core_name}' created successfully.")
    else:
        print(f"Failed to create core '{p_core_name}'. Error: {response.text}")


def indexData(p_core_name, p_exclude_column):
    try:
        with open('cleaned_employee_data.json') as f:
            employees = json.load(f)

        for employee in employees:
            if p_exclude_column in employee:
                del employee[p_exclude_column]

            response = requests.post(f'http://localhost:8983/solr/{p_core_name}/update/json/docs', json=employee)
            response.raise_for_status()

    except Exception as e:
        print(f"Error indexing data: {e}")


def searchByColumn(p_core_name, p_column_name, p_column_value):
    try:
        query = f'q={p_column_name}:{p_column_value}&wt=json'
        response = requests.get(f'http://localhost:8983/solr/{p_core_name}/select?{query}')
        response.raise_for_status()
        data = response.json()
        print(f"Search results for {p_column_name} = {p_column_value}:")
        for doc in data['response']['docs']:
            print(doc)
    except Exception as e:
        print(f"Error searching by column: {e}")


def getEmpCount(p_core_name):
    try:
        response = requests.get(f'http://localhost:8983/solr/{p_core_name}/select?q=*:*&rows=0')
        response.raise_for_status()
        data = response.json()
        count = data['response']['numFound']
        print(f"Employee count: {count}")
    except Exception as e:
        print(f"Error getting employee count: {e}")


def delEmpById(p_core_name, p_employee_id):
    try:
        response = requests.post(f'http://localhost:8983/solr/{p_core_name}/update?commit=true',
                                 json={'delete': {'id': p_employee_id}})
        response.raise_for_status()
        print(f"Deleted employee with ID: {p_employee_id}")
    except Exception as e:
        print(f"Error deleting employee by ID: {e}")


def getDepFacet(p_core_name):
    try:
        response = requests.get(f'http://localhost:8983/solr/{p_core_name}/facet?q=*:*&facet.field=Department')
        response.raise_for_status()
        data = response.json()
        facets = data['facet_counts']['facet_fields']['Department']
        print("Department facets:")
        for i in range(0, len(facets), 2):
            print(f"Department: {facets[i]}, Count: {facets[i + 1]}")
    except Exception as e:
        print(f"Error getting department facets: {e}")


# Main execution code
v_nameCollection = 'Hash_YourName'  # Replace with your actual name
v_phoneCollection = 'Hash_YourLastFourDigits'  # Replace with your actual phone last four digits

createCore(v_nameCollection)
createCore(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
delEmpById(v_nameCollection, 'E02003')  # Adjust the employee ID as needed
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
