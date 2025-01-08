import streamlit as st
from pydantic import BaseModel
from google.cloud import storage
import json

class MappingModel(BaseModel):
    gbq_output_table: str
    project_id: str
    region: str
    temp_location: str
    delimiter: str
    gbq_write_mode: str
    bucket_name: str
    file_name: str

def upload_mapping_json(data: MappingModel, overwrite: bool = True):
    bucket_name = data.bucket_name
    file_name = data.file_name

    mapping_dict = data.dict()
    json_content = json.dumps(mapping_dict, indent=4)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if blob.exists() and overwrite:
        blob.delete()

    if not blob.exists():
        blob.upload_from_string(json_content)
    else:
        st.error("File already exists and overwrite is set to False")
        return {"status": "failed", "message": "File already exists and overwrite is set to False"}

    return {"status": "success", "message": f"File {file_name} uploaded to bucket {bucket_name} with overwrite={overwrite}"}

def main():
    st.title("Upload Mapping JSON to Google Cloud Storage")

    gbq_output_table = st.text_input("GBQ Output Table")
    project_id = st.text_input("Project ID")
    region = st.text_input("Region")
    temp_location = st.text_input("Temp Location")
    delimiter = st.text_input("Delimiter")
    gbq_write_mode = st.text_input("GBQ Write Mode")
    bucket_name = st.text_input("Bucket Name")
    file_name = st.text_input("File Name")
    overwrite = st.checkbox("Overwrite if exists?", value=True)

    if st.button("Upload"):
        data = MappingModel(
            gbq_output_table=gbq_output_table,
            project_id=project_id,
            region=region,
            temp_location=temp_location,
            delimiter=delimiter,
            gbq_write_mode=gbq_write_mode,
            bucket_name=bucket_name,
            file_name=file_name
        )

        result = upload_mapping_json(data, overwrite)
        if result["status"] == "success":
            st.success(result["message"])
        else:
            st.error(result["message"])

if __name__ == "__main__":
    main()
