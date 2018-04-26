package kmean;

import java.io.Serializable;

public class Point{
	private double x;
	private double y;
	Point(double inputX,double inputY){
		this.x = inputX;
		this.y = inputY;
	}
	Point(){
		this.x = 500 + (int)(Math.random()*2000);
		this.y = 500 + (int)(Math.random()*2000);
	}
	Point(String s){
		String[] ss = s.split(";");
		this.x = Double.parseDouble(ss[0]);
		this.y = Double.parseDouble(ss[1]);
	}
	public double getX() {
		return this.x;
	}
	public double getY() {
		return this.y;
	}
	public double getDistance(Point other) {
		return Math.sqrt((x - other.getX())*(x - other.getX()) + (y - other.getY())*(y - other.getY()));
	}
	public String toString() {
		return (x+";"+y);
	}
}
