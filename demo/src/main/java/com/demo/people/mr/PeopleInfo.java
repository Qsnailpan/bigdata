package com.demo.people.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;;

public class PeopleInfo implements Writable {

	private int peopleID;
	private String gender;
	private int height;

	public PeopleInfo(int peopleID, String gender, int height) {
		super();
		this.peopleID = peopleID;
		this.gender = gender;
		this.height = height;
	}

	public PeopleInfo() {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(peopleID);
		out.writeUTF(gender);
		out.writeInt(height);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		peopleID = in.readInt();
		gender = in.readUTF();
		height = in.readInt();

	}

	public int getPeopleID() {
		return peopleID;
	}

	public void setPeopleID(int peopleID) {
		this.peopleID = peopleID;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	@Override
	public String toString() {
		return "PeopleInfo [peopleID=" + peopleID + ", gender=" + gender + ", height=" + height + "]";
	}

}
