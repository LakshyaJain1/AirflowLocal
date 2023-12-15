class User:
    def __init__(self, name, dob, pan, mobile, um_uuid, tenant_id):
        self.name = name
        self.dob = dob
        self.pan = pan
        self.mobile = mobile
        self.um_uuid = um_uuid
        self.tenant_id = tenant_id

    def __str__(self):
        return f"Name: {self.name}, DOB: {self.dob}, PAN: {self.pan}, UUID: {self.um_uuid}, Tenant Id: {self.tenant_id}"
