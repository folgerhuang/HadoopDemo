package com.sxt.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserWritable implements WritableComparable<UserWritable> {
	// 用户id
	private Integer id;
	// 用户收入
	private Integer income;
	// 用户支出
	private Integer expense;
	// 总的余额
	private Integer sum;

	public int getId() {
		return id;
	}

	public UserWritable setId(Integer id) {
		this.id = id;
		return this;
	}

	public int getIncome() {
		return income;
	}

	public UserWritable setIncome(Integer income) {
		this.income = income;
		return this;
	}

	public int getExpense() {
		return expense;
	}

	public UserWritable setExpense(Integer expense) {
		this.expense = expense;
		return this;
	}

	public int getSum() {
		return sum;
	}

	public UserWritable setSum(Integer sum) {
		this.sum = sum;
		return this;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(income);
		out.writeInt(expense);
		out.writeInt(sum);

	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.income = in.readInt();
		this.expense = in.readInt();
		this.sum = in.readInt();

	}

	public int compareTo(UserWritable o) {
		return this.id > o.getId() ? 1 : -1;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return id + "\t" + income + "\t" + expense + "\t" + sum;

	}
}
