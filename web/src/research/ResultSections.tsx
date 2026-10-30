// Every section from the results onwards. They are the ones that draw charts,
// so they are loaded as a separate chunk: the overview, architecture and
// method render without waiting for the charting library.
import { LoadSweep } from "./LoadSweep";
import { MainMatrix } from "./MainMatrix";
import { Isolation } from "./Isolation";
import { Cost } from "./Cost";
import { Failover } from "./Failover";
import { Fencing } from "./Fencing";
import { StoreFault } from "./StoreFault";
import { Partition } from "./Partition";
import { RealWork } from "./RealWork";
import { Corrections } from "./Corrections";
import { Limitations } from "./Limitations";
import { Reproduce } from "./Reproduce";

export default function ResultSections() {
  return (
    <>
      <LoadSweep />
      <MainMatrix />
      <Isolation />
      <Cost />
      <Failover />
      <Fencing />
      <StoreFault />
      <Partition />
      <RealWork />
      <Corrections />
      <Limitations />
      <Reproduce />
    </>
  );
}
