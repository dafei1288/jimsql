package net.fly78.miniSQL.library;

public class CartesianProduct {
	Object[] arrayA;
	Object[] arrayB;

	public CartesianProduct(Object[] arrayA, Object[] arrayB) {
		this.arrayA = arrayA;
		this.arrayB = arrayB;
	}

	public Pair[] cartesianProductAxB() {
		Pair[] product = new Pair[arrayA.length * arrayB.length];

		int amount = 0;
		for (int i = 0; i < arrayA.length; i++) {
			for (int j = 0; j < arrayB.length; j++) {
				product[amount++] = new Pair(arrayA[i], arrayB[j]);
			}
		}
		return product;
	}

	public Pair[] cartesianProductBxA() {
		Pair[] product = new Pair[arrayA.length * arrayB.length];

		int amount = 0;
		for (int i = 0; i < arrayB.length; i++) {
			for (int j = 0; j < arrayA.length; j++) {
				product[amount++] = new Pair(arrayB[i], arrayA[j]);
			}
		}
		return product;
	}

	static class Pair // Support class
	{
		private Object _first, _second;

		public Pair(Object first, Object second) {
			_first = first;
			_second = second;
		}

		public String toString() {
			return "<" + _first + ", " + _second + ">";
		}

		public boolean equals(Pair other) {
			return other._first.equals(this._first)
					&& other._second.equals(this._second);
		}
	}
	
	public static void main(String[] args){
		String[] a = {"jack","tom","alice","bob"};
		String[] b = {"China","English","USA","ITALIY","JAPAN"};
		CartesianProduct cp = new CartesianProduct(a,b);
		Pair[] p = cp.cartesianProductAxB();
		for(Pair pa : p){
			System.out.println(pa);
		}
	}
}
