package com.harvicom.kafkastreams.lookuptable.processor;

public class LookupTable {
    //private static final long serialVersionUID = 1L;
 
    public String Id, Status, Description;
 
    public String getId() {
        return Id;
    }


    public void setId(String id) {
        Id = id;
    }


    public String getStatus() {
        return Status;
    }


    public void setStatus(String status) {
        Status = status;
    }


    public String getDescription() {
        return Description;
    }


    public void setDescription(String description) {
        Description = description;
    }


    @Override
    public String toString()
    {
        return "Lookup Table [Id =" + Id + ", Status =" + Status + ",Description = " + Description  + "]";
    }
}    

